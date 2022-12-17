CREATE TABLE EVENT (
    id int NOT NULL,
    event_name varchar NULL,
    event_type varchar(50) NULL,
    organizer_id int NOT NULL,
    created_date date NOT NULL,
    PRIMARY KEY (id)
);