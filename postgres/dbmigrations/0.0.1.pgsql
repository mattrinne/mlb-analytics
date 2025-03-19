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