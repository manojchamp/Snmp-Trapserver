use tokio::net::UdpSocket;
use tokio::task;
use tokio::runtime::Builder;
use rdkafka::{ClientConfig, producer::{FutureProducer, FutureRecord}};
use rdkafka::error::KafkaError;  // Import KafkaError here
use rdkafka::util::Timeout;
use std::sync::Arc;
use tokio::sync::Mutex;

// #[tokio::main]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a custom runtime with a specific number of worker threads
    let rt = Builder::new_multi_thread()
        .worker_threads(4) // Set the number of worker threads for the async runtime
        .max_blocking_threads(25) // Set the number of threads in the blocking thread pool
        .enable_all()
        .build()?;

    // Use the runtime to run the async main function
    rt.block_on(async {
        // Create a UDP socket bound to port 162
        let socket = UdpSocket::bind("0.0.0.0:162").await?;
        println!("Listening on port 162...");

        let mut buf = [0u8; 1024]; // Buffer to hold received data

        // Initialize the Kafka producer wrapped in an Arc<Mutex>
        let producer = Arc::new(Mutex::new(create_kafka_producer()));

        loop {
            // Receive data from the socket
            let (len, addr) = socket.recv_from(&mut buf).await?;

            // Print the length of the received buffer
            println!("Received {} bytes from {}", len, addr);

            // Offload the processing of the received bytes to a blocking thread
            let data = buf[..len].to_vec(); // Clone the data (it will be passed to the thread pool)

            // Spawn a blocking task on the thread pool to process the data
            let producer_clone = Arc::clone(&producer); // Clone the Arc to move it into the closure
            let handle = task::spawn_blocking(move || {
                process_received_data(data, &producer_clone);
            });

            // Await the blocking task's completion
            handle.await?;
        }
    })
}

/// Function to process the received data and send it to Kafka
fn process_received_data(data: Vec<u8>, producer: &Arc<Mutex<FutureProducer>>) {
    // Simulate some processing on the received data (for example, print the data length)
    println!("Processing data with length: {}", data.len());

    // Send the data to Kafka topic "snmprawdata"
    let send_result = tokio::runtime::Runtime::new().unwrap().block_on(send_to_kafka(producer, data));

    match send_result {
        Ok(_) => println!("Successfully sent data to Kafka"),
        Err(e) => eprintln!("Failed to send data to Kafka: {}", e),
    }
}

/// Function to create a Kafka producer
fn create_kafka_producer() -> FutureProducer {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092") // Specify Kafka broker address
        .create()
        .expect("Kafka producer creation failed");
    producer
}

async fn send_to_kafka(producer: &Arc<Mutex<FutureProducer>>, data: Vec<u8>) -> Result<(), KafkaError> {
    // Lock the producer asynchronously
    let producer = producer.lock().await;  // Await the lock on the Mutex

    let record = FutureRecord::to("snmprawdata")
        .payload(&data)
        .key("key"); // You can change the key if needed

    // Send the data asynchronously to Kafka with no timeout (Timeout::Never)
    let result = producer.send(record, Timeout::Never).await;

    // Match on the result of the send operation
    match result {
        Ok((_, _)) => Ok(()),  // Success - return Ok(())
        Err((e, _)) => Err(e),  // Handle the error and ignore the OwnedMessage
    }
}