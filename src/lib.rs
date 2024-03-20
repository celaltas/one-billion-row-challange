use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufRead, BufReader, Error},
    sync::{Arc, Mutex},
};

use tokio::sync::{broadcast, mpsc};

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
    pub fn new(name: &'a str, values: &'a Vec<f32>) -> City<'a> {
        let mut mean = values.iter().sum();
        mean = mean / values.len() as f32;

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

pub fn run() -> Result<(), Error> {
    let mut cities: HashMap<String, Vec<f32>> = HashMap::new();
    let filename = "test.txt";

    match open(filename) {
        Ok(file) => {
            read_and_transform(file, &mut cities);
        }
        Err(err) => eprintln!("Failed open to {}: {}", filename, err),
    }

    let mut city_list = cities.keys().collect::<Vec<&String>>();
    city_list.sort();

    for city in city_list {
        let values = cities.get(city).unwrap();
        City::new(city, values);
    }

    Ok(())
}

fn read_and_transform(mut file: Box<dyn BufRead>, cities: &mut HashMap<String, Vec<f32>>) {
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
                .or_insert(Vec::new())
                .push(temp);
            buf.clear()
        }
    }
}

fn open(filename: &str) -> Result<Box<dyn BufRead>, Error> {
    Ok(Box::new(BufReader::new(File::open(filename)?)))
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
            let chunk_lines: Vec<String> = lines[start_line..end_line].iter().cloned().collect();

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

            let chunk_lines: Vec<String> = lines[start_line..end_line].iter().cloned().collect();
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
            .or_insert(Vec::new())
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
    let (tx, mut rx) = broadcast::channel::<Message>(lines_per_thread);
    let cities = Arc::new(Mutex::new(HashMap::<String, Vec<f32>>::new()));

    let senders: Vec<_> = (0..num_threads)
        .map(|i| {
            let start_line = i * lines_per_thread;
            let end_line = if i == num_threads - 1 {
                total_lines
            } else {
                (i + 1) * lines_per_thread
            };

            let chunk_lines: Vec<String> = lines[start_line..end_line].iter().cloned().collect();
            let tx_clone = tx.clone();

            tokio::spawn(async move {
                for line in chunk_lines {
                    let (city, temperature) = extract_city_temp(&line);
                    let message = Message { city, temperature };
                    println!("sended message = {:#?}", message);
                    if let Err(err) = tx_clone.send(message) {
                        eprintln!("Error sending message: {:?}", err);
                    }
                }
            })
        })
        .collect();

    let receivers: Vec<_> = (0..num_threads)
        .map(|i| {
            let mut rx_clone = tx.subscribe();
            let cities = Arc::clone(&cities);

            tokio::spawn(async move {
                for i in (0..lines_per_thread) {
                    let message = rx_clone.recv().await;
                    match message {
                        Ok(msg) => {
                            let mut guard = cities.lock().unwrap();
                            guard
                                .entry(msg.city)
                                .and_modify(|f| f.push(msg.temperature))
                                .or_insert_with(|| vec![msg.temperature]);
                        }
                        Err(err) => eprintln!("Error sending message: {:?}", err),
                    }
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
    let values: Vec<&str> = buf.trim_end().split(";").collect();
    let city = values.get(0).unwrap();
    let temp = values.get(1).unwrap();
    let temp = temp.parse::<f32>().unwrap();
    (city.to_string(), temp)
}
pub fn extract_city_temp_with_parser(buf: &str) -> (String, f32) {
    let values: Vec<&str> = buf.trim_end().split(";").collect();
    let city = values.get(0).unwrap();
    let temp = values.get(1).unwrap();
    let temp = float_parser(temp).unwrap();
    (city.to_string(), temp)
}


fn float_parser(input: &str) -> Option<f32> {
    let mut result = 0.0;
    let mut fraction = 0.0;
    let mut decimal_place = 0;
    let mut is_fractional = false;

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
            _ => return None, 
        }
    }

    if is_fractional {
        fraction /= 10.0_f32.powi(decimal_place as i32);
        result += fraction;
    }

    Some(result)
}



#[cfg(test)]
mod tests {
    use crate::{
        float_parser, read_by_threads_shared_data, read_by_threads_with_broadcast_channels, read_by_threads_with_mpsc_channels
    };

    #[test]
    fn test_float_parser(){

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
}