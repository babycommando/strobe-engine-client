pub struct Accum {
    cap: usize,
    mask: usize,
    keys: Vec<u32>,
    vals: Vec<f32>,
    tag: Vec<u32>,
    epoch: u32,
    touched: Vec<u32>,
}
impl Accum {
    pub fn with_capacity_pow2(pow2_capacity: usize) -> Self {
        assert!(pow2_capacity.is_power_of_two());
        let cap = pow2_capacity;
        Self {
            cap,
            mask: cap - 1,
            keys: vec![u32::MAX; cap],
            vals: vec![0.0; cap],
            tag: vec![0u32; cap],
            epoch: 1,
            touched: Vec::with_capacity(8192),
        }
    }
    #[inline(always)]
    pub fn begin(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            self.tag.fill(0);
            self.epoch = 1;
        }
        self.touched.clear();
    }
    #[inline(always)]
    pub fn inc(&mut self, id: u32) -> bool {
        let mut i = (id as usize) & self.mask;
        let mut probes = 0;
        loop {
            if probes >= self.cap { return false; }
            probes += 1;
            let t = unsafe { *self.tag.get_unchecked(i) };
            if t != self.epoch {
                unsafe {
                    *self.tag.get_unchecked_mut(i) = self.epoch;
                    *self.keys.get_unchecked_mut(i) = id;
                    *self.vals.get_unchecked_mut(i) = 0.0;
                }
                self.touched.push(id);
                return true;
            }
            if unsafe { *self.keys.get_unchecked(i) } == id { return false; }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    pub fn set_score(&mut self, id: u32, score: f32) {
        let mut i = (id as usize) & self.mask;
        loop {
            if unsafe { *self.keys.get_unchecked(i) } == id {
                unsafe { *self.vals.get_unchecked_mut(i) = score; }
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    pub fn update_max(&mut self, id: u32, score: f32) {
        let mut i = (id as usize) & self.mask;
        loop {
            if unsafe { *self.keys.get_unchecked(i) } == id {
                let v = unsafe { self.vals.get_unchecked_mut(i) };
                if score > *v { *v = score; }
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    pub fn get_score(&self, id: u32) -> f32 {
        let mut i = (id as usize) & self.mask;
        loop {
            if unsafe { *self.keys.get_unchecked(i) } == id {
                return unsafe { *self.vals.get_unchecked(i) };
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    pub fn iter_touched<'a>(&'a self) -> impl Iterator<Item = u32> + 'a {
        self.touched.iter().copied()
    }
}
impl Default for Accum {
    fn default() -> Self {
        let cap = 1 << 20;
        Self::with_capacity_pow2(cap)
    }
}
