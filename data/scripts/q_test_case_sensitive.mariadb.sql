-- Create a case-sensitive schema (database)
CREATE SCHEMA `WorldData`;

-- Case-Sensitive Schema and Table
CREATE TABLE `WorldData`.`Country`
(
    id   int,
    name varchar(20)
);

INSERT INTO `WorldData`.`Country` VALUES (1, 'India'), (2, 'USA'), (3, 'Japan'), (4, 'Germany');


-- Case-Sensitive Partition Column
CREATE TABLE `WorldData`.`Cities`
(
    id   int,
    name varchar(20),
    `RegionID` int
);
INSERT INTO `WorldData`.`Cities` VALUES (1, 'Mumbai', 10), (2, 'New York', 20), (3, 'Tokyo', 30), (4, 'Berlin', 40), (5, 'New Delhi', 10), (6, 'Kyoto', 30);


-- Case-Sensitive Query Field Names
CREATE TABLE `WorldData`.`Geography`
(
    id int,
    `Description` varchar(50)
);
INSERT INTO `WorldData`.`Geography` VALUES (1, 'Asia'), (2, 'North America'), (3, 'Asia'), (4, 'Europe');
