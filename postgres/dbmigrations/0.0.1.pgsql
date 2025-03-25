CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE IF NOT EXISTS metadata.sports (
    id INTEGER PRIMARY KEY,
    code VARCHAR(5),
    name VARCHAR(40),
    abbreviation VARCHAR(10),
    sort_order INTEGER,
    active BOOLEAN
);

CREATE TABLE IF NOT EXISTS metadata.leagues (
    id INTEGER PRIMARY KEY,
    name VARCHAR(40),
    sport_id INTEGER
);

INSERT INTO metadata.leagues (id, name, sport_id)
VALUES (103, 'American League', 1),
        (104, 'National League', 1)
;

CREATE TABLE IF NOT EXISTS metadata.divisions (
    id INTEGER PRIMARY KEY,
    name VARCHAR(40),
    league_id INTEGER
);

INSERT INTO metadata.divisions (id, name, league_id)
VALUES (200, 'American League West', 103),
        (201, 'American League East', 103),
        (202, 'American League Central', 103),
        (203, 'National League West', 104),
        (204, 'National League East', 104),
        (205, 'National League Central', 104)
;

CREATE TABLE IF NOT EXISTS metadata.teams (
    id INTEGER PRIMARY KEY,
    sport_id INTEGER,
    division_id INTEGER,
    name VARCHAR(40),
    team_code VARCHAR(10),
    file_code VARCHAR(10),
    abbreviation VARCHAR(6),
    team_name VARCHAR(30),
    location VARCHAR(30),
    active BOOLEAN
);

CREATE TABLE IF NOT EXISTS metadata.positions (
    code VARCHAR(5) PRIMARY KEY,
    short_name VARCHAR(25),
    full_name VARCHAR(25),
    abbreviation VARCHAR(5),
    type VARCHAR(25),
    formal_name VARCHAR(30),
    display_name VARCHAR(30),
    game_position BOOLEAN,
    pitcher BOOLEAN,
    fielder BOOLEAN,
    outfield BOOLEAN
);

CREATE TABLE IF NOT EXISTS metadata.event_types (
	code varchar(30) PRIMARY KEY,
	plate_appearance bool NULL,
	hit bool NULL,
	base_running_event bool NULL,
	description varchar(60) NULL
);

CREATE TABLE IF NOT EXISTS metadata.game_statuses (
    status_code VARCHAR(5) PRIMARY KEY,
    abstract_game_state VARCHAR(20),
    coded_game_state VARCHAR(5),
    detailed_state VARCHAR(45),
    abstract_game_code VARCHAR(5)
);

CREATE TABLE IF NOT EXISTS metadata.game_types (
    id VARCHAR(1) PRIMARY KEY,
    description VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS metadata.metrics (
    id INTEGER PRIMARY KEY,
    name VARCHAR(40),
    grouping VARCHAR(30),
    unit VARCHAR(6)
);