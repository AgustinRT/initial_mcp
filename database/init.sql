\c postgres
DROP DATABASE IF EXISTS quetionsdb WITH (FORCE);
CREATE DATABASE quetionsdb WITH OWNER = admin ENCODING = 'UTF8';
\c quetionsdb

drop table IF EXISTS recorded_tool CASCADE;
drop SEQUENCE IF EXISTS recorded_tool_seq;
CREATE SEQUENCE recorded_tool_seq;

CREATE TABLE recorded_tool (
    tool_id INTEGER NOT NULL DEFAULT nextval('recorded_tool_seq'),
    name_tool VARCHAR(255) NOT NULL,
    description_tool TEXT,
    input_params TEXT NULL,
    response_type TEXT NULL
);
ALTER TABLE recorded_tool ADD PRIMARY KEY (tool_id);   

ALTER TABLE recorded_tool 
ADD CONSTRAINT uq_recorded_tool_name_tool UNIQUE (name_tool);


drop table IF EXISTS question CASCADE;
drop SEQUENCE IF EXISTS question_seq;
CREATE SEQUENCE question_seq;

CREATE TABLE question (
    question_id INTEGER NOT NULL DEFAULT nextval('question_seq'),
    prompt TEXT NOT NULL
);
ALTER TABLE question ADD PRIMARY KEY (question_id);

drop table IF EXISTS awnser CASCADE;
drop SEQUENCE IF EXISTS awnser_seq;
CREATE SEQUENCE awnser_seq;

CREATE TABLE awnser (
    awnser_id INTEGER NOT NULL DEFAULT nextval('awnser_seq'),
    response TEXT NOT NULL,
    question_id INTEGER NOT NULL,
    tool_used_id INTEGER REFERENCES recorded_tool(tool_id) DEFAULT NULL,
    model_used TEXT DEFAULT NULL,
    cost_tokens TEXT DEFAULT NULL
);

ALTER TABLE awnser ADD PRIMARY KEY (awnser_id);
CREATE INDEX idx_awnser_tool_used ON awnser (tool_used_id) WHERE tool_used_id IS NOT NULL;

ALTER TABLE awnser ADD CONSTRAINT fk_question_id_question FOREIGN KEY (question_id) REFERENCES question(question_id);
ALTER TABLE awnser ADD CONSTRAINT fk_tool_used_id_tool FOREIGN KEY (tool_used_id) REFERENCES recorded_tool(tool_id);