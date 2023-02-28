--TYPE
CREATE TYPE change_status AS ENUM ('insert', 'update');

-- DATA SOURCE
CREATE TABLE IF NOT EXISTS data_source (
    id CHARACTER VARYING(50) PRIMARY KEY,
    title CHARACTER VARYING(150) NOT NULL,
    url CHARACTER VARYING(255) NOT NULL,
    description CHARACTER VARYING(255)
);


-- ORGANISATION UNIT
CREATE TABLE IF NOT EXISTS organisationunit (
    id CHARACTER VARYING(11),
    code CHARACTER VARYING(50),
    created TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    lastupdated TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    name CHARACTER VARYING(230) NOT NULL,
    shortname CHARACTER VARYING(50) NOT NULL,
    parentid CHARACTER VARYING(11),
    path CHARACTER VARYING(255),
    openingdate date,
    closeddate date,
    contactperson CHARACTER VARYING(255),
    address CHARACTER VARYING(255),
    email CHARACTER VARYING(150),
    phonenumber CHARACTER VARYING(150),
    change change_status default 'insert',
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (id, source_id),
    CONSTRAINT fk_organisationunit_data_source FOREIGN KEY(source_id) REFERENCES data_source(id)
);

CREATE OR REPLACE FUNCTION track_organisationunit_changes()
    RETURNS TRIGGER 
    LANGUAGE PLPGSQL
    AS
$$
BEGIN
	IF NEW.name <> OLD.name OR NEW.shortname <> OLD.shortname OR 
        NEW.parentid <> OLD.parentid THEN
		 UPDATE organisationunit SET change = 'update'
         WHERE id = NEW.id AND source_id =  NEW.source_id;
	END IF;

	RETURN NEW;
END;
$$;

CREATE TRIGGER organisationunit_changes_trigger
    AFTER UPDATE
    ON organisationunit
    FOR EACH ROW
    EXECUTE PROCEDURE track_organisationunit_changes();

-- CREATE TABLE IF NOT EXISTS period (
--     id CHARACTER VARYING(50) PRIMARY KEY,
--     title CHARACTER VARYING(150) NOT NULL,
--     url CHARACTER VARYING(255) NOT NULL,
--     description CHARACTER VARYING(255)
-- );


-- OPTION SET
CREATE TABLE IF NOT EXISTS optionset (
    id CHARACTER VARYING(11),
    code CHARACTER VARYING(50),
    created TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    lastupdated TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    name CHARACTER VARYING(230) NOT NULL,
    valuetype CHARACTER VARYING(50) NOT NULL,
    version integer,
    change change_status default 'insert',
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (id, source_id),
    CONSTRAINT fk_optionset_data_source FOREIGN KEY(source_id) REFERENCES data_source(id)
);

CREATE OR REPLACE FUNCTION track_optionset_changes()
    RETURNS TRIGGER 
    LANGUAGE PLPGSQL
    AS
$$
BEGIN
	IF NEW.name <> OLD.name THEN
		 UPDATE optionset SET change = 'update'
         WHERE id = NEW.id AND source_id =  NEW.source_id;
	END IF;

	RETURN NEW;
END;
$$;

CREATE TRIGGER optionset_changes_trigger
    AFTER UPDATE
    ON optionset
    FOR EACH ROW
    EXECUTE PROCEDURE track_optionset_changes();


-- CATEGORY COMBO
-- e.g. SexAge
CREATE TABLE IF NOT EXISTS categorycombo (
    id CHARACTER VARYING(11) NOT NULL,
    code CHARACTER VARYING(50),
    created TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    lastupdated TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    name CHARACTER VARYING(230) NOT NULL,
    change change_status default 'insert',
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (id, source_id),
    CONSTRAINT fk_categorycombo_data_source FOREIGN KEY(source_id) REFERENCES data_source(id)
);

CREATE OR REPLACE FUNCTION track_categorycombo_changes()
    RETURNS TRIGGER 
    LANGUAGE PLPGSQL
    AS
$$
BEGIN
	IF NEW.name <> OLD.name THEN
		 UPDATE categorycombo SET change = 'update'
         WHERE id = NEW.id AND source_id =  NEW.source_id;
	END IF;

	RETURN NEW;
END;
$$;

CREATE TRIGGER categorycombo_changes_trigger
    AFTER UPDATE
    ON categorycombo
    FOR EACH ROW
    EXECUTE PROCEDURE track_categorycombo_changes();


-- DATA ELEMENT CATEGORY
-- Sex, Age
CREATE TABLE IF NOT EXISTS  dataelementcategory (
    id CHARACTER VARYING(11),
    code CHARACTER VARYING(50),
    created TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    lastupdated TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    name CHARACTER VARYING(230) NOT NULL,
    datadimension boolean,
    change change_status default 'insert',
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (id, source_id),
    CONSTRAINT fk_dataelementcategory_data_source FOREIGN KEY(source_id) REFERENCES data_source(id)
);

CREATE OR REPLACE FUNCTION track_dataelementcategory_changes()
    RETURNS TRIGGER 
    LANGUAGE PLPGSQL
    AS
$$
BEGIN
	IF NEW.name <> OLD.name OR NEW.datadimension <> OLD.datadimension THEN
		 UPDATE dataelementcategory SET change = 'update'
         WHERE id = NEW.id AND source_id =  NEW.source_id;
	END IF;

	RETURN NEW;
END;
$$;

CREATE TRIGGER dataelementcategory_changes_trigger
    AFTER UPDATE
    ON dataelementcategory
    FOR EACH ROW
    EXECUTE PROCEDURE track_dataelementcategory_changes();


-- DATA ELEMENT CATEGORY OPTION
-- Male, Female, 0-4, 4-15, ...
CREATE TABLE IF NOT EXISTS dataelementcategoryoption (
    id CHARACTER VARYING(11),
    code CHARACTER VARYING(50),
    created TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    lastupdated TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    name CHARACTER VARYING(230) NOT NULL,
    shortname CHARACTER VARYING(50) NOT NULL,
    startdate date,
    enddate date,
    dataelementcategory_id CHARACTER VARYING(11),
    change change_status default 'insert', 
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (id, source_id),
    CONSTRAINT fk_dataelementcategoryoption_dataelementcategory FOREIGN KEY(dataelementcategory_id) REFERENCES dataelementcategory(id),
    CONSTRAINT fk_dataelementcategoryoption_data_source FOREIGN KEY(source_id) REFERENCES data_source(id)
);

CREATE OR REPLACE FUNCTION track_dataelementcategoryoption_changes()
    RETURNS TRIGGER 
    LANGUAGE PLPGSQL
    AS
$$
BEGIN
	IF NEW.name <> OLD.name OR NEW.shortname <> OLD.shortname THEN
		 UPDATE dataelementcategoryoption SET change = 'update'
         WHERE id = NEW.id AND source_id =  NEW.source_id;
	END IF;

	RETURN NEW;
END;
$$;

CREATE TRIGGER dataelementcategoryoption_changes_trigger
    AFTER UPDATE
    ON dataelementcategoryoption
    FOR EACH ROW
    EXECUTE PROCEDURE track_dataelementcategoryoption_changes();


-- DATA ELEMENT CATEGORY - CATEGORY COMBO
-- Many to Many between categorycombo and dataelementcategory
-- Sex => SexAge
-- Age => SexAge
CREATE TABLE IF NOT EXISTS dataelementcategory_categorycombo (
    categorycombo_id CHARACTER VARYING(11),
    dataelementcategory_id CHARACTER VARYING(11),
    change change_status default 'insert', 
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (categorycombo_id, dataelementcategory_id, source_id),
    CONSTRAINT fk_dataelementcategorycombo_categorycombo FOREIGN KEY(categorycombo_id) REFERENCES categorycombo(id),
    CONSTRAINT fk_dataelementcategorycombo_dataelementcategory FOREIGN KEY(dataelementcategory_id) REFERENCES dataelementcategory(id),
    CONSTRAINT fk_dataelementcategory_categorycombo_data_source FOREIGN KEY(source_id) REFERENCES data_source(id)
);


-- CATEGORY OPTION COMBO
-- Male 0-4, Male 5-15, Female 0-4, Female 5-15, ...
CREATE TABLE IF NOT EXISTS categoryoptioncombo (
    id CHARACTER VARYING(11),
    name CHARACTER VARYING(230) NOT NULL,
    categorycombo_id CHARACTER VARYING(11),
    change change_status default 'insert', 
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (id, source_id),
    CONSTRAINT fk_categoryoptioncombo_categorycombo FOREIGN KEY(categorycombo_id) REFERENCES categorycombo(id),
    CONSTRAINT fk_categoryoptioncombo_data_source FOREIGN KEY(source_id) REFERENCES data_source(id)
);

CREATE OR REPLACE FUNCTION track_categoryoptioncombo_changes()
    RETURNS TRIGGER 
    LANGUAGE PLPGSQL
    AS
$$
BEGIN
	IF NEW.name <> OLD.name THEN
		 UPDATE categoryoptioncombo SET change = 'update'
         WHERE id = NEW.id AND source_id =  NEW.source_id;
	END IF;

	RETURN NEW;
END;
$$;

CREATE TRIGGER categoryoptioncombo_changes_trigger
    AFTER UPDATE
    ON categoryoptioncombo
    FOR EACH ROW
    EXECUTE PROCEDURE track_categoryoptioncombo_changes();


-- DATA ELEMENT
CREATE TABLE IF NOT EXISTS dataelement (
    id CHARACTER VARYING(11) NOT NULL,
    code CHARACTER VARYING(50),
    created TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    lastupdated TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    name CHARACTER VARYING(230) NOT NULL,
    shortname CHARACTER VARYING(50) NOT NULL,
    formname CHARACTER VARYING(230),
    valuetype CHARACTER VARYING(50) NOT NULL,
    domaintype CHARACTER VARYING(255) NOT NULL,
    aggregationtype CHARACTER VARYING (50) NOT NULL,
    categorycomboid BIGINT NOT NULL,
    url CHARACTER VARYING(255),
    optionsetid BIGINT,
    change change_status default 'insert', 
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (id, source_id),
    CONSTRAINT fk_dataelement_optionset FOREIGN KEY(optionsetid) REFERENCES optionset(id),
    CONSTRAINT fk_dataelement_categorycombo FOREIGN KEY(categorycomboid) REFERENCES categorycombo(id),
    CONSTRAINT fk_dataelement_data_source FOREIGN KEY(source_id) REFERENCES data_source(id)
);

CREATE OR REPLACE FUNCTION track_dataelement_changes()
    RETURNS TRIGGER 
    LANGUAGE PLPGSQL
    AS
$$
BEGIN
	IF NEW.name <> OLD.name OR NEW.shortname <> OLD.shortname OR 
        NEW.formname <> OLD.formname OR NEW.domaintype <> OLD.domaintype OR 
        NEW.aggregationtype <> OLD.aggregationtype OR NEW.categorycomboid <> OLD.categorycomboid THEN
		 UPDATE dataelement SET change = 'update'
         WHERE id = NEW.id AND source_id =  NEW.source_id;
	END IF;

	RETURN NEW;
END;
$$;

CREATE TRIGGER dataelement_changes_trigger
    AFTER UPDATE
    ON dataelement
    FOR EACH ROW
    EXECUTE PROCEDURE track_dataelement_changes();


-- DATA VALUE
CREATE TABLE IF NOT EXISTS datavalue (
    dataelementid CHARACTER VARYING(11) NOT NULL,
    period CHARACTER VARYING(50) NOT NULL,
    organisationunitid CHARACTER VARYING(11) NOT NULL,
    categoryoptioncomboid CHARACTER VARYING(11) NOT NULL,
    attributeoptioncomboid CHARACTER VARYING(11) NOT NULL,
    created TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    lastupdated TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    value CHARACTER VARYING(50000),
    followup boolean,
    deleted boolean NOT NULL,
    change change_status default 'insert', 
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (dataelementid, period, organisationunitid, categoryoptioncomboid, attributeoptioncomboid, source_id),
    CONSTRAINT fk_dataelement_datavalue FOREIGN KEY(dataelementid) REFERENCES dataelement(id),
    CONSTRAINT fk_organisationunit_datavalue FOREIGN KEY(organisationunitid) REFERENCES organisationunit(id),
    CONSTRAINT fk_categoryoptioncombo_datavalue FOREIGN KEY(categoryoptioncomboid) REFERENCES categoryoptioncombo(id),
    CONSTRAINT fk_categoryoptioncombo_datavalue_2 FOREIGN KEY(attributeoptioncomboid) REFERENCES categoryoptioncombo(id),
    CONSTRAINT fk_source_datavalue FOREIGN KEY(source_id) REFERENCES data_source(id)
);

CREATE OR REPLACE FUNCTION track_datavalue_changes()
    RETURNS TRIGGER 
    LANGUAGE PLPGSQL
    AS
$$
BEGIN
	IF NEW.value <> OLD.value THEN
		 UPDATE datavalue SET change = 'update'
         WHERE dataelementid = NEW.dataelementid AND period =  NEW.period AND organisationunitid = NEW.organisationunitid AND
                categoryoptioncomboid = NEW.categoryoptioncomboid AND attributeoptioncomboid = NEW.attributeoptioncomboid AND
                source_id = NEW.source_id;
	END IF;

	RETURN NEW;
END;
$$;

CREATE TRIGGER datavalue_changes_trigger
    AFTER UPDATE
    ON datavalue
    FOR EACH ROW
    EXECUTE PROCEDURE track_datavalue_changes();
