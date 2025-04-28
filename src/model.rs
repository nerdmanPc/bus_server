
use chrono::prelude::*;
use geo::Point;
//use sqlite::{Connection, State, Statement};

#[derive(Debug, PartialEq, Copy, Clone)]
pub struct Record {
    pub bus_id: u64,
    pub timestamp: DateTime<Utc>,
    pub position: Point,
    pub doors_open: bool,
}

pub trait DataBase {
    fn add_record(&self, record: &Record);
    fn update_status(&self, record: &Record);
    #[cfg(test)] fn list_records(&self) -> Vec<Record>;
    #[cfg(test)] fn list_status(&self) -> Vec<Record>;
}

pub struct SQLiteDataBase {
    connection:  sqlite::Connection,
}

impl Default for SQLiteDataBase {
    fn default() -> Self {
        let connection = sqlite::open(":memory:").expect("Error creating memory database!");
        let schema_query = include_str!("sql/schema.sql");
        connection.execute(schema_query).expect("Error initializing database!");
        Self { connection }
    }
}

impl SQLiteDataBase {
    
    fn from_path(path: &str) -> Self {
        let connection = sqlite::open(path).expect("Error connecting to database!");
        Self { connection }
    }

    fn update_table(&self, record: &Record, table: &str) {
        let query = format!("INSERT OR REPLACE INTO {table} VALUES (?, ?, ?, ?, ?);");
        let bus_id = record.bus_id;
        let timestamp = record.timestamp.to_rfc3339();
        let (latitude, longitude) = record.position.into();
        let doors_open = if record.doors_open { 1 } else { 0 };

        let mut statement = self.connection
            .prepare(query)
            .expect("Failed to prepare query!");

        statement.bind((1, bus_id as i64)).expect("Failed to bind bus_id");
        statement.bind((2, timestamp.as_str())).expect("Failed to bind timestamp");
        statement.bind((3, latitude)).expect("Failed to bind latitude");
        statement.bind((4, longitude)).expect("Failed to bind longitude");
        statement.bind((5, doors_open)).expect("Failed to bind doors_open");

        //use AsRef<str>;
        while let sqlite::State::Row = statement.next().expect("Error running statement!") {  }
    }

    #[cfg(test)]
    fn list_table(&self, table: &str) -> Vec<Record> {
        let query = format!("SELECT * FROM {table};");
        let statement = self.connection.prepare(query).expect("Failed to prepare query!");
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
            let bus_id = bus_id as u64;
            Record {
                bus_id,
                timestamp,
                position,
                doors_open,
            }
        }).collect();
        result
    }
}

impl DataBase for SQLiteDataBase {

    fn add_record(&self, record: &Record) {
        self.update_table(record, "records");
    }

    fn update_status(&self, record: &Record) {
        self.update_table(record, "status");
    } 

    #[cfg(test)]
    fn list_records(&self) -> Vec<Record> {
        self.list_table("records")
    }

    #[cfg(test)]
    fn list_status(&self) -> Vec<Record> {
        self.list_table("status")
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::*;

    #[test]
    fn should_add_first_record() {
        let records_table = SQLiteDataBase::default();
        let record: Record = Record {
            bus_id: 0,
            timestamp: DateTime::<Utc>::MIN_UTC,
            position: Point::new(0.0, 0.0),
            doors_open: true
        };

        records_table.add_record(&record);
        assert_eq!(records_table.list_records(), vec![record])
    }

    #[test]
    fn should_add_first_status() {
        let records_table = SQLiteDataBase::default();
        let record: Record = Record {
            bus_id: 0,
            timestamp: DateTime::<Utc>::MIN_UTC,
            position: Point::new(0.0, 0.0),
            doors_open: true
        };

        records_table.update_status(&record);
        assert_eq!(records_table.list_status(), vec![record])
    }

    #[test]
    fn should_add_different_bus_record() {
        let records_table = SQLiteDataBase::default();
        let record_a: Record = Record {
            bus_id: 0,
            timestamp: DateTime::<Utc>::MIN_UTC,
            position: Point::new(0.0, 0.0),
            doors_open: true
        };

        let record_b: Record = Record {
            bus_id: 1,
            timestamp: DateTime::<Utc>::MIN_UTC,
            position: Point::new(0.0, 0.0),
            doors_open: true
        };

        records_table.add_record(&record_a);
        records_table.add_record(&record_b);
        assert!(records_table.list_records().contains(&record_a));
        assert!(records_table.list_records().contains(&record_b));
    }

    #[test]
    fn should_add_different_bus_status() {
        let records_table = SQLiteDataBase::default();
        let record_a: Record = Record {
            bus_id: 0,
            timestamp: DateTime::<Utc>::MIN_UTC,
            position: Point::new(0.0, 0.0),
            doors_open: true
        };

        let record_b: Record = Record {
            bus_id: 1,
            timestamp: DateTime::<Utc>::MIN_UTC,
            position: Point::new(0.0, 0.0),
            doors_open: true
        };

        records_table.update_status(&record_a);
        records_table.update_status(&record_b);
        assert!(records_table.list_status().contains(&record_a));
        assert!(records_table.list_status().contains(&record_b));
    }

    #[test]
    fn should_add_different_time_record() {
        let records_table = SQLiteDataBase::default();
        let record_a: Record = Record {
            bus_id: 0,
            timestamp: DateTime::<Utc>::MIN_UTC,
            position: Point::new(0.0, 0.0),
            doors_open: true
        };

        let record_b: Record = Record {
            bus_id: 0,
            timestamp: DateTime::<Utc>::MIN_UTC + TimeDelta::seconds(5),
            position: Point::new(0.0, 0.0),
            doors_open: true
        };

        records_table.add_record(&record_a);
        records_table.add_record(&record_b);
        assert!(records_table.list_records().contains(&record_a));
        assert!(records_table.list_records().contains(&record_b));
    }

    #[test]
    fn should_replace_same_bus_status() {
        let records_table = SQLiteDataBase::default();
        let record_a: Record = Record {
            bus_id: 0,
            timestamp: DateTime::<Utc>::MIN_UTC,
            position: Point::new(0.0, 0.0),
            doors_open: true
        };

        let record_b: Record = Record {
            bus_id: 0,
            timestamp: DateTime::<Utc>::MIN_UTC + TimeDelta::seconds(5),
            position: Point::new(0.0, 0.0),
            doors_open: true
        };

        records_table.update_status(&record_a);
        records_table.update_status(&record_b);
        assert_eq!(records_table.list_status(), vec![record_b]);
    }
}