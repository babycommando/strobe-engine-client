use std::{
    collections::HashMap,
    net::{SocketAddr, UdpSocket},
    time::{Duration, Instant},
};

use quiche::h3::{self, NameValue};

const MAX_DATAGRAM_SIZE: usize = 1350;

// Simple static random CID generator
fn random_cid() -> quiche::ConnectionId<'static> {
    let mut id = [0u8; 16];
    let now = Instant::now().elapsed().as_nanos();
    for (i, b) in id.iter_mut().enumerate() {
        *b = ((now >> (i % 8) * 8) as u8).wrapping_add(i as u8);
    }
    quiche::ConnectionId::from_vec(id.to_vec())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listen = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:4433".to_string());
    let cert = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "cert.pem".to_string());
    let key = std::env::args()
        .nth(3)
        .unwrap_or_else(|| "key.pem".to_string());

    let socket = UdpSocket::bind(&listen)?;
    socket.set_nonblocking(true)?;
    eprintln!("listening on {listen}");

    let mut cfg = quiche::Config::new(quiche::PROTOCOL_VERSION)?;
    cfg.set_application_protos(quiche::h3::APPLICATION_PROTOCOL)?;
    cfg.set_max_idle_timeout(10_000);
    cfg.set_initial_max_data(1_000_000_000);
    cfg.set_initial_max_stream_data_bidi_local(1_000_000_000);
    cfg.set_initial_max_stream_data_bidi_remote(1_000_000_000);
    cfg.set_initial_max_streams_bidi(1_000_000);
    cfg.set_initial_max_streams_uni(1_000_000);
    cfg.enable_early_data();
    cfg.set_disable_active_migration(true);
    cfg.load_cert_chain_from_pem_file(&cert)?;
    cfg.load_priv_key_from_pem_file(&key)?;

    let h3_cfg = quiche::h3::Config::new()?;

    let mut conns: HashMap<SocketAddr, (quiche::Connection, Option<h3::Connection>)> = HashMap::new();

    let mut in_buf = [0u8; 64 * 1024];
    let mut out_buf = [0u8; 64 * 1024];

    let start_time = Instant::now();
    let mut last_log = start_time;
    let mut total_reqs: u64 = 0;

    loop {
        // === Receive all available packets ===
        loop {
            match socket.recv_from(&mut in_buf) {
                Ok((read, from)) => {
                    let entry = conns.entry(from).or_insert_with(|| {
                        let scid = random_cid();
                        let local = socket.local_addr().unwrap();
                        let conn = quiche::accept(&scid, None, local, from, &mut cfg).unwrap();
                        (conn, None)
                    });

                    let recv_info = quiche::RecvInfo {
                        from,
                        to: socket.local_addr().unwrap(),
                    };

                    if let Err(e) = entry.0.recv(&mut in_buf[..read], recv_info) {
                        if e != quiche::Error::Done {
                            eprintln!("recv error: {e:?}");
                        }
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(Box::new(e)),
            }
        }

        // === Process each connection ===
        let mut to_remove = Vec::new();

        for (&peer_addr, (conn, h3_opt)) in conns.iter_mut() {
            conn.on_timeout();

            // Establish H3 layer
            if h3_opt.is_none() && conn.is_established() {
                *h3_opt = Some(h3::Connection::with_transport(conn, &h3_cfg).unwrap());
            }

            if let Some(h3c) = h3_opt.as_mut() {
                loop {
                    match h3c.poll(conn) {
                        Ok((stream_id, h3::Event::Headers { list, .. })) => {
                            let mut path = None;
                            for h in &list {
                                if h.name() == b":path" {
                                    path = Some(h.value().to_vec());
                                }
                            }
                            if path.as_deref() == Some(b"/hello") {
                                total_reqs += 1;
                                let resp = vec![
                                    h3::Header::new(b":status", b"200"),
                                    h3::Header::new(b"server", b"quiche"),
                                    h3::Header::new(b"content-type", b"text/plain"),
                                ];
                                let _ = h3c.send_response(conn, stream_id, &resp, false);
                                let _ = h3c.send_body(conn, stream_id, b"hello\n", true);
                            } else {
                                let resp = vec![
                                    h3::Header::new(b":status", b"404"),
                                    h3::Header::new(b"server", b"quiche"),
                                ];
                                let _ = h3c.send_response(conn, stream_id, &resp, true);
                            }
                        }
                        Ok((_id, h3::Event::Data)) => {}
                        Err(h3::Error::Done) => break,
                        Err(e) => {
                            eprintln!("h3.poll err: {e:?}");
                            conn.close(true, 0x100, b"h3err").ok();
                            break;
                        }
                        _ => {}
                    }
                }
            }

            // Flush pending QUIC packets
            loop {
                match conn.send(&mut out_buf) {
                    Ok((write, send_info)) => {
                        let _ = socket.send_to(&out_buf[..write.min(MAX_DATAGRAM_SIZE)], send_info.to);
                    }
                    Err(quiche::Error::Done) => break,
                    Err(e) => {
                        eprintln!("conn.send err: {e:?}");
                        conn.close(true, 0x100, b"senderr").ok();
                        break;
                    }
                }
            }

            if conn.is_closed() {
                to_remove.push(peer_addr);
            }
        }

        for addr in to_remove {
            conns.remove(&addr);
        }

        // Periodic logging (once per second)
        let now = Instant::now();
        if now.duration_since(last_log) >= Duration::from_secs(1) {
            let elapsed_s = now.duration_since(start_time).as_secs();
            eprintln!(
                "[{}s] total_reqs={} conns={}",
                elapsed_s,
                total_reqs,
                conns.len()
            );
            last_log = now;
        }
    }
}

