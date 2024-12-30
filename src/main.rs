use core::time;
use std::time::SystemTime;
use chrono::prelude::*;
use geo::Point;
use sqlite::{Connection, State, Statement};

fn main() {
    println!("Hello, world!");
}

#[derive(Debug, PartialEq, Copy, Clone)]
struct Record {
    bus_id: i64,
    timestamp: DateTime<Utc>,
    position: Point,
    doors_open: bool,
}

fn add_record(connection: &Connection, record: Record) {
    let query = "INSERT INTO records VALUES (?, ?, ?, ?, ?);";
    let bus_id = record.bus_id;
    let timestamp = record.timestamp.to_rfc3339();
    let (latitude, longitude) = record.position.into();
    let doors_open = if record.doors_open { 1 } else { 0 };

    let mut statement = connection
        .prepare(query)
        .expect("Failed to prepare query!");
        
    statement.bind((1, bus_id)).expect("Failed to bind bus_id");
    statement.bind((2, timestamp.as_str())).expect("Failed to bind timestamp");
    statement.bind((3, latitude)).expect("Failed to bind latitude");
    statement.bind((4, longitude)).expect("Failed to bind longitude");
    statement.bind((5, doors_open)).expect("Failed to bind doors_open");

    //use AsRef<str>;
    while let State::Row = statement.next().expect("Error running statement!") {  }
}
fn list_records(connection: &Connection) -> Vec<Record> {
    let query = "SELECT * FROM records;";
    let statement = connection.prepare(query).expect("Failed to prepare query!");
    let result = statement.into_iter().filter(|row| { row.is_ok() }).map(|row| {
        let row = row.unwrap();

        let bus_id: i64 = row.read("bus_id");
        let timestamp: &str = row.read("timestamp");
        let latitude: f64 = row.read("latitude");
        let longitude: f64 = row.read("longitude");
        let doors_open: i64 = row.read("doors_open");
        
        let timestamp = timestamp.parse().expect(format!("Failed to parse timestamp [{}]!", timestamp).as_str());
        let position = Point::new(latitude, longitude);
        let doors_open = doors_open != 0 ;
        Record {
            bus_id,
            timestamp,
            position,
            doors_open,
        }
    }).collect();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_add_record() {
        let database = sqlite::open(":memory:").expect("Error creating memory database!");
        let schema_query = include_str!("sql/schema.sql");
        database.execute(schema_query).expect("Error initializing database!");
        let record = Record {
            bus_id: 0,
            timestamp: DateTime::<Utc>::MIN_UTC,
            position: Point::new(0.0, 0.0),
            doors_open: true
        };
        add_record(&database, record);
        assert_eq!(list_records(&database), vec![record])
    }
}