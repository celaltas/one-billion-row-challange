use rustc_hash::FxHashMap;
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::{self, BufRead, BufReader, Error},
    sync::{Arc, Mutex},
    time::Instant,
};
use threadpool::ThreadPool;
use tokio::sync::{broadcast, mpsc};

pub struct Stats {
    min: f32,
    max: f32,
    sum: f32,
    count: f32,
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            min: f32::INFINITY,
            max: f32::NEG_INFINITY,
            sum: 0.0,
            count: 0.0,
        }
    }
}

pub struct City<'a> {
    name: &'a str,
    min: &'a f32,
    mean: f32,
    max: &'a f32,
}

#[derive(Debug, Clone)]
pub struct Message {
    pub city: String,
    pub temperature: f32,
}

impl<'a> City<'a> {
    pub fn new(name: &'a str, values: &'a [f32]) -> City<'a> {
        let mut mean = values.iter().sum();
        mean /= values.len() as f32;

        City {
            name,
            min: values.first().unwrap(),
            mean,
            max: values.last().unwrap(),
        }
    }

    pub fn print_info(&self) {
        println!("{}={}/{}/{}", self.name, self.min, self.mean, self.max)
    }
}

pub async fn run() -> io::Result<()> {
    let filename = "measurements.txt";
    let reader = BufReader::new(File::open(filename)?);
    let chunk_size = 8000;
    let chunks = Arc::new(Mutex::new(Vec::with_capacity(chunk_size)));
    let cities = Arc::new(Mutex::new(BTreeMap::<String, Stats>::new()));
    let mut total = 0;
    let pool = ThreadPool::new(24);

    for line in reader.lines() {
        let line = line.unwrap();
        chunks.lock().unwrap().push(line);

        if chunks.lock().unwrap().len() == chunk_size {
            let cities = Arc::clone(&cities);
            let chunks_clone = Arc::clone(&chunks);
            // let handle = tokio::spawn(async move {
            //     process_chunk(chunks_clone, cities).await;
            // });
            // handle.await?;
            pool.execute( || {
                process_chunk(chunks_clone, cities)
            });
            total += chunk_size;
            println!("total = {total}");
        }
    }

    let cities_guard = cities.lock().unwrap();
    for (city, stats) in cities_guard.iter() {
        let mean = stats.sum / stats.count;
        println!("{}={}/{}/{}", city, stats.min, mean, stats.max);
    }

    Ok(())
}

fn process_chunk(
    chunks: Arc<Mutex<Vec<String>>>,
    cities: Arc<Mutex<BTreeMap<String, Stats>>>,
) {
    let mut stats = BTreeMap::<String, Stats>::new();
    let mut chunks = chunks.lock().unwrap();
    for line in chunks.iter() {
        let (city, temp) = extract_city_temp_with_parser(&line);
        let city_stats = stats.entry(city).or_default();
        city_stats.min = temp.min(city_stats.min);
        city_stats.max = temp.max(city_stats.max);
        city_stats.sum += temp;
        city_stats.count += 1.0;
    }
    chunks.clear();
    let mut cities = cities.lock().unwrap();
    for (city, stat) in stats {
        let city_stats = cities.entry(city).or_default();
        city_stats.min = stat.min.min(city_stats.min);
        city_stats.max = stat.max.max(city_stats.max);
        city_stats.sum += stat.sum;
        city_stats.count += stat.count;
    }
}

fn read_exact_line(filename: &str, start_line: usize, num_lines: usize) -> Vec<String> {
    let reader = BufReader::new(File::open(filename).unwrap());

    let lines = reader
        .lines()
        .skip(start_line)
        .take(num_lines)
        .map(|l| l.unwrap())
        .collect();
    lines
}

pub fn read_by_single_thread_with_btree() -> Result<(), Error> {
    let mut cities: BTreeMap<String, Stats> = BTreeMap::new();
    let filename = "measurements.txt";
    let file = File::open(filename)?;
    let mut file = BufReader::new(file);
    let mut buf = String::new();
    let mut total = 0;
    loop {
        let byte_read = file.read_line(&mut buf).unwrap();
        if byte_read == 0 {
            break;
        } else {
            let (city, temp) = extract_city_temp(&buf);
            let city_stats = cities.entry(city).or_default();
            city_stats.min = temp.min(city_stats.min);
            city_stats.max = temp.max(city_stats.max);
            city_stats.sum += temp;
            city_stats.count += 1.0;
            buf.clear();
            total += 1;
            println!("{total}");
        }
    }

    for (city, stats) in cities.into_iter() {
        let mean = stats.sum / stats.count;
        println!("{}={}/{}/{}", city, stats.min, mean, stats.max);
    }

    Ok(())
}

pub fn read_by_single_thread_with_hashmap_stats() -> Result<(), Error> {
    let mut cities: HashMap<String, Stats> = HashMap::new();
    let filename = "test.txt";
    let file = File::open(filename)?;
    let mut file = BufReader::new(file);
    let mut buf = String::new();
    loop {
        let byte_read = file.read_line(&mut buf).unwrap();
        if byte_read == 0 {
            break;
        } else {
            let (city, temp) = extract_city_temp(&buf);
            let city_stats = cities.entry(city).or_default();
            city_stats.min = temp.min(city_stats.min);
            city_stats.max = temp.max(city_stats.max);
            city_stats.sum += temp;
            city_stats.count += 1.0;
            buf.clear()
        }
    }
    let mut city_list = cities.keys().collect::<Vec<&String>>();
    city_list.sort();
    for city in city_list {
        let values = cities.get(city).unwrap();
        let mean = values.sum / values.count;
        println!("{}={}/{}/{}", city, values.min, mean, values.max);
    }

    Ok(())
}

pub fn read_by_single_thread_with_hashmap() -> Result<(), Error> {
    let mut cities: HashMap<String, Vec<f32>> = HashMap::new();
    let filename = "test.txt";
    let file = File::open(filename)?;
    let mut file = BufReader::new(file);
    let mut buf = String::new();
    loop {
        let byte_read = file.read_line(&mut buf).unwrap();
        if byte_read == 0 {
            break;
        } else {
            let (city, temp) = extract_city_temp(&buf);
            cities
                .entry(city.to_string())
                .and_modify(|f| f.push(temp))
                .or_default()
                .push(temp);
            buf.clear()
        }
    }
    let mut city_list = cities.keys().collect::<Vec<&String>>();
    city_list.sort();
    for city in city_list {
        let values = cities.get(city).unwrap();
        City::new(city, values).print_info();
    }

    Ok(())
}

pub fn read_by_single_thread_with_fast_hasher() -> Result<(), Error> {
    let mut cities: FxHashMap<String, Vec<f32>> = FxHashMap::default();
    let filename = "test.txt";
    let file = File::open(filename)?;
    let mut file = BufReader::new(file);

    let mut buf = String::new();
    loop {
        let byte_read = file.read_line(&mut buf).unwrap();
        if byte_read == 0 {
            break;
        } else {
            let (city, temp) = extract_city_temp(&buf);
            cities
                .entry(city.to_string())
                .and_modify(|f| f.push(temp))
                .or_default()
                .push(temp);
            buf.clear()
        }
    }

    let mut city_list = cities.keys().collect::<Vec<&String>>();
    city_list.sort();

    for city in city_list {
        let values = cities.get(city).unwrap();
        City::new(city, values);
    }

    Ok(())
}

pub async fn read_by_threads_shared_data() -> io::Result<()> {
    let filename = "test.txt";
    let num_threads = num_cpus::get();
    let reader = BufReader::new(File::open(filename)?);
    let lines: Vec<_> = reader.lines().map(|line| line.unwrap()).collect();
    let total_lines = lines.len();
    let lines_per_thread = total_lines / num_threads;

    let cities = Arc::new(Mutex::new(HashMap::<String, Vec<f32>>::new()));

    let handles: Vec<_> = (0..num_threads)
        .map(|i| {
            let start_line = i * lines_per_thread;
            let end_line = if i == num_threads - 1 {
                total_lines
            } else {
                (i + 1) * lines_per_thread
            };

            let cities = Arc::clone(&cities);
            let chunk_lines: Vec<String> = lines[start_line..end_line].to_vec();

            tokio::spawn(async move {
                for line in chunk_lines {
                    let (city, temp) = extract_city_temp(&line);
                    let mut guard = cities.lock().unwrap();
                    guard
                        .entry(city.to_string())
                        .and_modify(|f| f.push(temp))
                        .or_insert_with(|| vec![temp]);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.await?;
    }

    let mut city_list = cities
        .lock()
        .unwrap()
        .keys()
        .cloned()
        .collect::<Vec<String>>();
    city_list.sort();

    for city in city_list {
        let values = cities.lock().unwrap().get(&city).unwrap().clone();
        City::new(&city, &values);
    }

    Ok(())
}

pub async fn read_by_threads_with_mpsc_channels() -> io::Result<()> {
    let filename = "test.txt";
    let num_threads = num_cpus::get();
    let reader = BufReader::new(File::open(filename)?);
    let lines: Vec<_> = reader.lines().map(|line| line.unwrap()).collect();
    let total_lines = lines.len();
    let lines_per_thread = total_lines / num_threads;
    let (tx, mut rx) = mpsc::channel::<Message>(lines_per_thread);
    let mut cities: HashMap<String, Vec<f32>> = HashMap::new();

    let handles: Vec<_> = (0..num_threads)
        .map(|i| {
            let start_line = i * lines_per_thread;
            let end_line = if i == num_threads - 1 {
                total_lines
            } else {
                (i + 1) * lines_per_thread
            };

            let chunk_lines: Vec<String> = lines[start_line..end_line].to_vec();
            let tx_clone = tx.clone();

            tokio::spawn(async move {
                for line in chunk_lines {
                    let (city, temperature) = extract_city_temp(&line);
                    let message = Message { city, temperature };
                    if let Err(err) = tx_clone.send(message).await {
                        eprintln!("Error sending message: {:?}", err);
                    }
                }
            })
        })
        .collect();

    drop(tx);
    while let Some(message) = rx.recv().await {
        cities
            .entry(message.city)
            .and_modify(|f| f.push(message.temperature))
            .or_default()
            .push(message.temperature);
    }

    for handle in handles {
        handle.await?;
    }

    let mut city_list = cities.keys().collect::<Vec<&String>>();
    city_list.sort();

    for city in city_list {
        let values = cities.get(city).unwrap();
        City::new(city, values);
    }

    Ok(())
}

pub async fn read_by_threads_with_broadcast_channels() -> io::Result<()> {
    let filename = "test.txt";
    let num_threads = num_cpus::get();
    let reader = BufReader::new(File::open(filename)?);
    let lines: Vec<_> = reader.lines().map(|line| line.unwrap()).collect();
    let total_lines = lines.len();
    let lines_per_thread = total_lines / num_threads;
    let (tx, _rx) = broadcast::channel::<Message>(lines_per_thread);
    let cities = Arc::new(Mutex::new(HashMap::<String, Vec<f32>>::new()));

    let senders: Vec<_> = (0..num_threads)
        .map(|i| {
            let start_line = i * lines_per_thread;
            let end_line = if i == num_threads - 1 {
                total_lines
            } else {
                (i + 1) * lines_per_thread
            };

            let chunk_lines: Vec<String> = lines[start_line..end_line].to_vec();
            let tx_clone = tx.clone();

            tokio::spawn(async move {
                for line in chunk_lines {
                    let (city, temperature) = extract_city_temp(&line);
                    let message = Message { city, temperature };
                    if let Err(err) = tx_clone.send(message) {
                        eprintln!("Error sending message: {:?}", err);
                    }
                }
            })
        })
        .collect();

    let receivers: Vec<_> = (0..num_threads)
        .map(|_| {
            let mut rx_clone = tx.subscribe();
            let cities_clone = Arc::clone(&cities);

            tokio::spawn(async move {
                while let Ok(msg) = rx_clone.recv().await {
                    let mut guard = cities_clone.lock().unwrap();
                    guard
                        .entry(msg.city)
                        .and_modify(|f| f.push(msg.temperature))
                        .or_insert_with(|| vec![msg.temperature]);
                }
            })
        })
        .collect();

    drop(tx);

    for handle in senders {
        handle.await?;
    }
    for handle in receivers {
        handle.await?;
    }

    let mut city_list = cities
        .lock()
        .unwrap()
        .keys()
        .cloned()
        .collect::<Vec<String>>();
    city_list.sort();

    for city in city_list {
        let values = cities.lock().unwrap().get(&city).unwrap().clone();
        City::new(&city, &values);
    }

    Ok(())
}

pub fn extract_city_temp(buf: &str) -> (String, f32) {
    let values: Vec<&str> = buf.trim_end().split(';').collect();
    let city = values.first().unwrap();
    let temp = values.get(1).unwrap();
    let temp = temp.parse::<f32>().unwrap();
    (city.to_string(), temp)
}

pub fn extract_city_temp_with_parser(buf: &str) -> (String, f32) {
    let values: Vec<&str> = buf.trim_end().split(';').collect();
    let city = values.first().unwrap();
    let temp = values.get(1).unwrap();
    let temp = float_parser(temp).unwrap();
    (city.to_string(), temp)
}

pub fn float_parser(input: &str) -> Option<f32> {
    let mut result = 0.0;
    let mut fraction = 0.0;
    let mut decimal_place = 0;
    let mut is_fractional = false;
    let mut is_negative = false;

    for c in input.chars() {
        match c {
            '0'..='9' => {
                let digit = (c as u8 - b'0') as f32;
                if is_fractional {
                    fraction = fraction * 10.0 + digit;
                    decimal_place += 1;
                } else {
                    result = result * 10.0 + digit;
                }
            }
            '.' => {
                is_fractional = true;
            }
            '-' => {
                is_negative = true;
            }
            _ => return None,
        }
    }

    if is_fractional {
        fraction /= 10.0_f32.powi(decimal_place as i32);
        result += fraction;
    }
    if is_negative {
        result *= -1.0;
    }

    Some(result)
}

#[cfg(test)]
mod tests {
    use crate::{
        float_parser, read_by_single_thread_with_btree, read_by_single_thread_with_fast_hasher,
        read_by_threads_shared_data, read_by_threads_with_broadcast_channels,
        read_by_threads_with_mpsc_channels, run,
    };

    #[test]
    fn test_float_parser() {
        let input = "abc";
        let res = float_parser(input);
        assert!(res.is_none());
        let input = "12,3";
        let res = float_parser(input);
        assert!(res.is_none());
        let input = "12.3";
        let res = float_parser(input);
        assert_eq!(12.3, res.unwrap());
        let input = "11321.3";
        let res = float_parser(input);
        assert_eq!(11321.3, res.unwrap());
        let input = "-11.3";
        let res = float_parser(input);
        assert_eq!(-11.3, res.unwrap());
    }

    #[test]
    fn test_read_by_single_thread_with_btree() {
        let res = read_by_single_thread_with_btree();
        assert!(res.is_ok())
    }
    #[test]
    fn test_read_by_single_thread_with_fast_hasher() {
        let res = read_by_single_thread_with_fast_hasher();
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_gread_by_threads_shared_data() {
        let res = read_by_threads_shared_data().await;
        assert!(res.is_ok())
    }
    #[tokio::test]
    async fn test_read_by_threads_with_mpsc_channels() {
        let res = read_by_threads_with_mpsc_channels().await;
        assert!(res.is_ok())
    }
    #[tokio::test]
    async fn test_read_by_threads_with_broadcast_channels() {
        let res = read_by_threads_with_broadcast_channels().await;
        assert!(res.is_ok())
    }
    #[tokio::test]
    async fn test_run() {
        let res = run().await;
        assert!(res.is_ok())
    }
}
