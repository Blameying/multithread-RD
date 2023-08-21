use hist_statis::Hist;
use stack_alg_sim::olken::LRUSplay;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::hash::Hash;
use std::mem;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::JoinHandle;
use std::{thread, time};

static MINIMAL_THRESHOLD: usize = 3;

pub struct Parda<T> {
    threshold: usize,
    counter: usize,
    msgs: Sender<usize>,
    sender: Option<Sender<T>>,
    infinity_bound: usize,
    local_rds: Arc<Mutex<HashMap<usize, HashMap<T, usize>>>>,
    local_infinities: Arc<Mutex<HashMap<usize, Vec<T>>>>,
    pub result: Arc<Mutex<HashMap<Option<usize>, usize>>>,
    threads: Vec<thread::JoinHandle<()>>,
    submit_order: Arc<AtomicUsize>,
    submit_thread: Option<JoinHandle<()>>,
}

impl<T: Send + Hash + Clone + Copy + Eq + std::fmt::Debug + 'static> Parda<T> {
    /* the threshold define the number of data will be processed in single thread */
    /* the reuse distance beyond infinity_bound will be treated as infinities */
    pub fn new(threshold: usize, infinity_bound: usize) -> Self {
        let (tx, rx): (Sender<usize>, Receiver<usize>) = mpsc::channel();
        let mut parda = Parda {
            threshold: if threshold > MINIMAL_THRESHOLD {
                threshold
            } else {
                MINIMAL_THRESHOLD
            },
            counter: 0,
            msgs: tx,
            sender: None,
            local_rds: Arc::new(Mutex::new(HashMap::new())),
            local_infinities: Arc::new(Mutex::new(HashMap::new())),
            result: Arc::new(Mutex::new(HashMap::new())),
            threads: Vec::new(),
            submit_order: Arc::new(AtomicUsize::new(0)),
            submit_thread: None,
            infinity_bound,
        };

        let local_rds = parda.local_rds.clone();
        let infinities = parda.local_infinities.clone();
        let result = parda.result.clone();

        let handle = thread::spawn(move || {
            let mut reduced_data_to_id: Box<HashMap<T, usize>> = Box::default();
            let mut reduced_id_to_data: Box<BTreeMap<usize, T>> = Box::default();

            loop {
                let id = rx.recv();
                if let Ok(id) = id {
                    let mut infinities_lock = infinities.lock().unwrap();
                    let local_infinities = infinities_lock.remove(&id).unwrap();
                    let mut position = 0;
                    /* reduce infinities */
                    loop {
                        if position == local_infinities.len() {
                            break;
                        }
                        if let Some(value) = reduced_data_to_id.get(&local_infinities[position]) {
                            let count = reduced_id_to_data.range((value + 1)..).count();
                            result
                                .lock()
                                .unwrap()
                                .entry(Some(count + 1)) // The minimal rd is set 1
                                .and_modify(|e| *e += 1)
                                .or_insert(1);
                            reduced_id_to_data.remove(value);
                        } else {
                            result
                                .lock()
                                .unwrap()
                                .entry(None)
                                .and_modify(|e| *e += 1)
                                .or_insert(1);
                        }

                        let mut new_id: usize = 0;
                        if let Some(pair) = reduced_id_to_data.last_key_value() {
                            new_id = pair.0 + 1;
                        }
                        reduced_id_to_data.insert(new_id, local_infinities[position]);
                        reduced_data_to_id
                            .entry(local_infinities[position])
                            .and_modify(|t| *t = new_id)
                            .or_insert(new_id);

                        /* bound limitation */
                        if infinity_bound > 0 && reduced_id_to_data.len() > infinity_bound {
                            let value = reduced_id_to_data.pop_first().unwrap().1;
                            reduced_data_to_id.remove(&value);
                        }
                        position += 1;
                    }
                    /* merge future rd */
                    let rds = local_rds.lock().unwrap().remove(&id).unwrap();
                    /* extend operation will overwrite the original value */
                    let mut sorted_vector: Vec<(&T, &usize)> = rds.iter().collect();
                    sorted_vector.sort_by_key(|(_, &v)| v);
                    let new_id = reduced_id_to_data.last_key_value().unwrap().0 + 1;

                    for (index, v) in sorted_vector.iter().enumerate() {
                        let old_id = reduced_data_to_id.get(v.0);
                        if let Some(old_id) = old_id {
                            reduced_id_to_data.remove(old_id);
                        }
                        reduced_id_to_data.insert(index + new_id, *v.0);

                        reduced_data_to_id
                            .entry(*v.0)
                            .and_modify(|t| *t = index + new_id)
                            .or_insert(index + new_id);
                    }

                    if infinity_bound > 0 {
                        while reduced_id_to_data.len() > infinity_bound {
                            let value = reduced_id_to_data.pop_first().unwrap().1;
                            reduced_data_to_id.remove(&value);
                        }
                    }
                } else {
                    break;
                }
            }
        });
        parda.submit_thread = Some(handle);

        parda
    }

    pub fn access(&mut self, val: T) {
        let rx = if self.sender.is_none() {
            let (tx, rx) = mpsc::channel();
            self.sender = Some(tx);
            Some(rx)
        } else if self.counter >= self.threshold {
            let (tx, rx) = mpsc::channel();
            self.sender = Some(tx);
            self.counter = 0;
            Some(rx)
        } else {
            None
        };

        if let Some(rx) = rx {
            let id = self.threads.len();
            let result = Arc::clone(&self.result);
            let infinity = Arc::clone(&self.local_infinities);
            let rd = Arc::clone(&self.local_rds);
            let finish = self.msgs.clone();
            let submit_order = Arc::clone(&self.submit_order);
            let infinity_bound = self.infinity_bound;

            /* create a new thread */
            let handle = thread::spawn(move || {
                let mut local_infinities: Vec<T> = Vec::new();
                let mut processer: LRUSplay<T> = LRUSplay::new();
                let mut hist: Hist<usize> = Hist::new();

                let mut queue: Vec<T> = Vec::new();
                let mut last_access: HashMap<T, usize> = HashMap::new();

                loop {
                    let data = rx.recv();
                    if let Ok(data) = data {
                        queue.push(data);
                    } else {
                        break;
                    }
                }

                /* statistic rd */
                for (index_of_element, &i) in queue.iter().enumerate() {
                    let result = processer.access(i);
                    if let Some(result) = result {
                        if infinity_bound == 0 || result <= infinity_bound {
                            hist.add_dist(Some(result));
                        } else {
                            hist.add_dist(None);
                        }
                    } else {
                        local_infinities.push(i);
                    }
                    last_access
                        .entry(i)
                        .and_modify(|t| *t = index_of_element)
                        .or_insert(index_of_element);
                }

                /* add scope to release the lock */
                {
                    let mut result = result.lock().unwrap();
                    for (key, value) in hist.hist {
                        result
                            .entry(key)
                            .and_modify(|v| *v += value)
                            .or_insert(value);
                    }
                }

                {
                    let mut infinity = infinity.lock().unwrap();
                    infinity.insert(id, local_infinities);
                }

                {
                    let mut rds = rd.lock().unwrap();
                    rds.insert(id, last_access);
                }

                while submit_order.load(Ordering::SeqCst) != id {
                    thread::sleep(time::Duration::from_millis(1));
                }
                finish.send(id).unwrap();
                submit_order.fetch_add(1, Ordering::SeqCst);
            });
            self.threads.push(handle);
        }

        self.sender.as_ref().unwrap().send(val).unwrap();
        self.counter += 1;
    }

    pub fn reduction(&mut self) {
        {
            /* release the sender */
            self.sender = None;
            (self.msgs, _) = mpsc::channel();
        }
        let threads_handles = mem::take(&mut self.threads);
        for t in threads_handles {
            let _ = t.join();
        }
        let submit_thread = mem::take(&mut self.submit_thread);
        let _ = submit_thread.unwrap().join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parda_without_rd_range_limitation() {
        let datas: Vec<u64> = vec![1, 3, 1, 1, 4, 2, 1, 2, 3];
        let mut parda: Parda<u64> = Parda::new(3, 0);
        for i in datas {
            parda.access(i);
        }
        parda.reduction();
        println!("{:?}", parda.result);
        let result = parda.result.lock().unwrap();
        assert_eq!(*result.get(&Some(1)).unwrap(), 1usize);
        assert_eq!(*result.get(&Some(4)).unwrap(), 1usize);
        assert_eq!(*result.get(&None).unwrap(), 4usize);
    }

    #[test]
    fn parda_with_rd_range_limitation() {
        let datas: Vec<u64> = vec![1, 3, 1, 1, 4, 2, 1, 2, 3];
        let mut parda: Parda<u64> = Parda::new(3, 2);
        for i in datas {
            parda.access(i);
        }
        parda.reduction();
        println!("{:?}", parda.result);
        let result = parda.result.lock().unwrap();
        assert_eq!(*result.get(&Some(1)).unwrap(), 1usize);
        assert_eq!(*result.get(&Some(2)).unwrap(), 2usize);
        assert_eq!(*result.get(&None).unwrap(), 6usize);
    }
}
