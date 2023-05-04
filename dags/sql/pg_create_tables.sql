--TYPE
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'change_status') THEN
        CREATE TYPE change_status AS ENUM ('insert', 'update', '');
    END IF;
END$$;


-- DATA SOURCE
CREATE TABLE IF NOT EXISTS data_source (
    id CHARACTER VARYING(50) PRIMARY KEY,
    title CHARACTER VARYING(150) NOT NULL,
    url CHARACTER VARYING(255) NOT NULL,
    description CHARACTER VARYING(255),
    change change_status default 'insert'
);

CREATE OR REPLACE FUNCTION track_data_source_changes()
    RETURNS TRIGGER 
    LANGUAGE PLPGSQL
    AS
$$
BEGIN
	IF NEW.id <> OLD.id OR NEW.title <> OLD.title OR 
        NEW.url <> OLD.url or NEW.description <> OLD.description THEN
		 UPDATE data_source SET change = 'update'
         WHERE uid = NEW.uid;
	END IF;

	RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER odata_source_changes_trigger
    AFTER UPDATE
    ON data_source
    FOR EACH ROW
    EXECUTE PROCEDURE track_data_source_changes();


-- ORGANISATION UNIT
CREATE TABLE IF NOT EXISTS organisationunit (
    uid CHARACTER VARYING(11),
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
    leaf BOOLEAN,
    featuretype character varying(255),
    coordinates text,
    change change_status default 'insert',
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (uid, source_id),
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
         WHERE uid = NEW.uid AND source_id =  NEW.source_id;
	END IF;

	RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER organisationunit_changes_trigger
    AFTER UPDATE
    ON organisationunit
    FOR EACH ROW
    EXECUTE PROCEDURE track_organisationunit_changes();


-- ORGUNIT LEVEL
CREATE TABLE IF NOT EXISTS orgunitlevel (
    uid CHARACTER VARYING(11),
    code CHARACTER VARYING(50),
    created TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    lastupdated TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    name CHARACTER VARYING(230) NOT NULL,
    previous_name CHARACTER VARYING(230),
    level integer NOT NULL,
    change change_status default 'insert',
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (uid, source_id)
);

CREATE OR REPLACE FUNCTION track_orgunitlevel_changes()
    RETURNS TRIGGER 
    LANGUAGE PLPGSQL
    AS
$$
BEGIN
	IF NEW.name <> OLD.name OR NEW.level <> OLD.level THEN
		 UPDATE orgunitlevel SET change = 'update', previous_name = OLD.name
         WHERE uid = NEW.uid AND source_id =  NEW.source_id;
	END IF;

	RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER orgunitlevel_changes_trigger
    AFTER UPDATE
    ON orgunitlevel
    FOR EACH ROW
    EXECUTE PROCEDURE track_orgunitlevel_changes();


-- CREATE TABLE IF NOT EXISTS period (
--     id CHARACTER VARYING(50) PRIMARY KEY,
--     title CHARACTER VARYING(150) NOT NULL,
--     url CHARACTER VARYING(255) NOT NULL,
--     description CHARACTER VARYING(255)
-- );


-- OPTION SET
CREATE TABLE IF NOT EXISTS optionset (
    uid CHARACTER VARYING(11),
    code CHARACTER VARYING(50),
    created TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    lastupdated TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    name CHARACTER VARYING(230) NOT NULL,
    valuetype CHARACTER VARYING(50) NOT NULL,
    version integer,
    change change_status default 'insert',
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (uid, source_id),
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
         WHERE uid = NEW.uid AND source_id =  NEW.source_id;
	END IF;

	RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER optionset_changes_trigger
    AFTER UPDATE
    ON optionset
    FOR EACH ROW
    EXECUTE PROCEDURE track_optionset_changes();


-- CATEGORY COMBO
-- e.g. SexAge
CREATE TABLE IF NOT EXISTS categorycombo (
    uid CHARACTER VARYING(11) NOT NULL,
    created TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    lastupdated TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    name CHARACTER VARYING(230) NOT NULL,
    change change_status default 'insert',
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (uid, source_id),
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
         WHERE uid = NEW.uid AND source_id =  NEW.source_id;
	END IF;

	RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER categorycombo_changes_trigger
    AFTER UPDATE
    ON categorycombo
    FOR EACH ROW
    EXECUTE PROCEDURE track_categorycombo_changes();


-- DATA ELEMENT CATEGORY
-- Sex, Age
CREATE TABLE IF NOT EXISTS  dataelementcategory (
    uid CHARACTER VARYING(11),
    created TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    lastupdated TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    name CHARACTER VARYING(230) NOT NULL,
    previous_name CHARACTER VARYING(230),
    datadimension boolean,
    change change_status default 'insert',
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (uid, source_id),
    CONSTRAINT fk_dataelementcategory_data_source FOREIGN KEY(source_id) REFERENCES data_source(id)
);

CREATE OR REPLACE FUNCTION track_dataelementcategory_changes()
    RETURNS TRIGGER 
    LANGUAGE PLPGSQL
    AS
$$
BEGIN
	IF NEW.name <> OLD.name OR NEW.datadimension <> OLD.datadimension THEN
		 UPDATE dataelementcategory SET change = 'update', previous_name = OLD.name
         WHERE uid = NEW.uid AND source_id =  NEW.source_id;
	END IF;

	RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER dataelementcategory_changes_trigger
    AFTER UPDATE
    ON dataelementcategory
    FOR EACH ROW
    EXECUTE PROCEDURE track_dataelementcategory_changes();


-- DATA ELEMENT CATEGORY OPTION
-- Male, Female, 0-4, 4-15, ...
CREATE TABLE IF NOT EXISTS dataelementcategoryoption (
    uid CHARACTER VARYING(11),
    created TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    lastupdated TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    name CHARACTER VARYING(230) NOT NULL,
    shortname CHARACTER VARYING(50),
    change change_status default 'insert', 
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (uid, source_id),
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
         WHERE uid = NEW.uid AND source_id =  NEW.source_id;
	END IF;

	RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER dataelementcategoryoption_changes_trigger
    AFTER UPDATE
    ON dataelementcategoryoption
    FOR EACH ROW
    EXECUTE PROCEDURE track_dataelementcategoryoption_changes();

-- DATA ELEMENT CATEGORY - CATEGORY OPTION
-- Many to Many between dataelementcategory and dataelementcategoryoption
-- Sex => M
-- Sex => F
-- Age => 0-4
CREATE TABLE IF NOT EXISTS dataelementcategory_categoryoption (
    category_id CHARACTER VARYING(11),
    categoryoption_id CHARACTER VARYING(11),
    change change_status default 'insert', 
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (category_id, categoryoption_id, source_id),
    CONSTRAINT fk_dataelementcategory_categoryoption_dataelementcategory FOREIGN KEY(category_id, source_id) REFERENCES dataelementcategory(uid, source_id),
    CONSTRAINT fk_dataelementcategory_categoryoption_dataelementcategoryoption FOREIGN KEY(categoryoption_id, source_id) REFERENCES dataelementcategoryoption(uid, source_id),
    CONSTRAINT fk_dataelementcategory_categoryoption_data_source FOREIGN KEY(source_id) REFERENCES data_source(id)
);

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
    CONSTRAINT fk_dataelementcategorycombo_categorycombo FOREIGN KEY(categorycombo_id, source_id) REFERENCES categorycombo(uid, source_id),
    CONSTRAINT fk_dataelementcategorycombo_dataelementcategory FOREIGN KEY(dataelementcategory_id, source_id) REFERENCES dataelementcategory(uid, source_id),
    CONSTRAINT fk_dataelementcategory_categorycombo_data_source FOREIGN KEY(source_id) REFERENCES data_source(id)
);


-- CATEGORY OPTION COMBO
-- Male 0-4, Male 5-15, Female 0-4, Female 5-15, ...
CREATE TABLE IF NOT EXISTS categoryoptioncombo (
    uid CHARACTER VARYING(11),
    name CHARACTER VARYING(230) NOT NULL,
    created TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    lastupdated TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    categorycombo_id CHARACTER VARYING(11),
    change change_status default 'insert', 
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (uid, source_id),
    CONSTRAINT fk_categoryoptioncombo_categorycombo FOREIGN KEY(categorycombo_id, source_id) REFERENCES categorycombo(uid, source_id),
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
         WHERE uid = NEW.uid AND source_id =  NEW.source_id;
	END IF;

	RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER categoryoptioncombo_changes_trigger
    AFTER UPDATE
    ON categoryoptioncombo
    FOR EACH ROW
    EXECUTE PROCEDURE track_categoryoptioncombo_changes();


--CATEGORY OPTION COMBO - CATEGORY OPTIONS

CREATE TABLE IF NOT EXISTS categoryoptioncombo_categoryoptions (
    category_option_combo_id CHARACTER VARYING(11),
    category_option_id CHARACTER VARYING(11),
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (category_option_combo_id, category_option_id, source_id),
    CONSTRAINT fk_categoryoptioncombo_categoryoptions_categoryoptioncombo FOREIGN KEY(category_option_combo_id, source_id) REFERENCES categoryoptioncombo(uid, source_id),
    CONSTRAINT fk_categoryoptioncombo_categoryoptions_dataelementcategoryoption FOREIGN KEY(category_option_id, source_id) REFERENCES dataelementcategoryoption(uid, source_id),
    CONSTRAINT fk_categoryoptioncombo_categoryoptions_data_source FOREIGN KEY(source_id) REFERENCES data_source(id)
);

-- DATA ELEMENT
CREATE TABLE IF NOT EXISTS dataelement (
    uid CHARACTER VARYING(11) NOT NULL,
    code CHARACTER VARYING(50),
    created TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    lastupdated TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    name CHARACTER VARYING(230) NOT NULL,
    shortname CHARACTER VARYING(50) NOT NULL,
    formname CHARACTER VARYING(230),
    valuetype CHARACTER VARYING(50) NOT NULL,
    domaintype CHARACTER VARYING(255) NOT NULL,
    aggregationtype CHARACTER VARYING (50) NOT NULL,
    categorycomboid CHARACTER VARYING(11) NOT NULL,
    url CHARACTER VARYING(255),
    optionsetid CHARACTER VARYING(11),
    change change_status default 'insert', 
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (uid, source_id),
    CONSTRAINT fk_dataelement_optionset FOREIGN KEY(optionsetid, source_id) REFERENCES optionset(uid, source_id),
    CONSTRAINT fk_dataelement_categorycombo FOREIGN KEY(categorycomboid, source_id) REFERENCES categorycombo(uid, source_id),
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
         WHERE uid = NEW.uid AND source_id =  NEW.source_id;
	END IF;

	RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER dataelement_changes_trigger
    AFTER UPDATE
    ON dataelement
    FOR EACH ROW
    EXECUTE PROCEDURE track_dataelement_changes();


-- DATA ELEMENT GROUP
CREATE TABLE IF NOT EXISTS dataelementgroup (
    uid CHARACTER VARYING(11) NOT NULL,
    created TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    lastupdated TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    name CHARACTER VARYING(230) NOT NULL,
    previous_name CHARACTER VARYING(230),
    formname CHARACTER VARYING(230),
    aggregationtype CHARACTER VARYING (50),
    change change_status default 'insert', 
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (uid, source_id),
    CONSTRAINT fk_dataelementgroup_data_source FOREIGN KEY(source_id) REFERENCES data_source(id)
);

CREATE OR REPLACE FUNCTION track_dataelementgroup_changes()
    RETURNS TRIGGER 
    LANGUAGE PLPGSQL
    AS
$$
BEGIN
	IF NEW.name <> OLD.name OR NEW.formname <> OLD.formname OR 
        NEW.aggregationtype <> OLD.aggregationtype THEN
		 UPDATE dataelementgroup SET change = 'update', previous_name = OLD.name
         WHERE uid = NEW.uid AND source_id =  NEW.source_id;
	END IF;

	RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER dataelementgroup_changes_trigger
    AFTER UPDATE
    ON dataelementgroup
    FOR EACH ROW
    EXECUTE PROCEDURE track_dataelementgroup_changes();


-- DATA ELEMENT - DATA ELEMENT GROUP
-- Many to Many between dataelement and dataelementgroup
-- Malaria => Total Pf
-- Malaria => Total pv
CREATE TABLE IF NOT EXISTS dataelement_dataelementgroup (
    dataelement_id CHARACTER VARYING(11),
    group_id CHARACTER VARYING(11),
    change change_status default 'insert', 
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (dataelement_id, group_id, source_id),
    CONSTRAINT fk_dataelement_dataelementgroup_dataelement FOREIGN KEY(dataelement_id, source_id) REFERENCES dataelement(uid, source_id),
    CONSTRAINT fk_dataelement_dataelementgroup_dataelementgroup FOREIGN KEY(group_id, source_id) REFERENCES dataelementgroup(uid, source_id),
    CONSTRAINT fk_dataelement_dataelementgroup_data_source FOREIGN KEY(source_id) REFERENCES data_source(id)
);

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
    deleted boolean,
    storedby CHARACTER VARYING(230),
    change change_status default 'insert', 
    source_id CHARACTER VARYING(50),
    PRIMARY KEY (dataelementid, period, organisationunitid, categoryoptioncomboid, attributeoptioncomboid, source_id),
    CONSTRAINT fk_dataelement_datavalue FOREIGN KEY(dataelementid, source_id) REFERENCES dataelement(uid, source_id),
    CONSTRAINT fk_organisationunit_datavalue FOREIGN KEY(organisationunitid, source_id) REFERENCES organisationunit(uid, source_id),
    CONSTRAINT fk_categoryoptioncombo_datavalue FOREIGN KEY(categoryoptioncomboid, source_id) REFERENCES categoryoptioncombo(uid, source_id),
    CONSTRAINT fk_categoryoptioncombo_datavalue_2 FOREIGN KEY(attributeoptioncomboid, source_id) REFERENCES categoryoptioncombo(uid, source_id),
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

CREATE OR REPLACE TRIGGER datavalue_changes_trigger
    AFTER UPDATE
    ON datavalue
    FOR EACH ROW
    EXECUTE PROCEDURE track_datavalue_changes();
