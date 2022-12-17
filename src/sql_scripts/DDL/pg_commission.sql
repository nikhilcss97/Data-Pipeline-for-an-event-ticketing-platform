CREATE TABLE COMMISSION (
    organizer_id int NOT NULL,
    event_id int NOT NULL,
    reseller_id int NOT NULL,
    commission_rate float NOT NULL,
    created_date date NOT NULL,
    PRIMARY KEY (organizer_id, event_id, reseller_id)
);