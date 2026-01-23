
-- Enums

CREATE TABLE ccy_enum (
  code CHAR(3) PRIMARY KEY,
  numeric_code CHAR(3) NULL,
  name VARCHAR(128) NOT NULL,
  minor_units SMALLINT NULL,
  is_active BOOLEAN NOT NULL,
  withdrawal_date VARCHAR(32) NULL
);

CREATE UNIQUE INDEX ux_ccy_enum_numeric_code ON ccy_enum(numeric_code);

INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ADP', '020', 'Andorran Peseta', NULL, FALSE, '2003-07');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('AED', '784', 'UAE Dirham', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('AFA', '004', 'Afghani', NULL, FALSE, '2003-01');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('AFN', '971', 'Afghani', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ALK', '008', 'Old Lek', NULL, FALSE, '1989-12');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ALL', '008', 'Lek', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('AMD', '051', 'Armenian Dram', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ANG', '532', 'Netherlands Antillean Guilder', NULL, FALSE, '2025-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('AOA', '973', 'Kwanza', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('AOK', '024', 'Kwanza', NULL, FALSE, '1991-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('AON', '024', 'New Kwanza', NULL, FALSE, '2000-02');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('AOR', '982', 'Kwanza Reajustado', NULL, FALSE, '2000-02');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ARA', '032', 'Austral', NULL, FALSE, '1992-01');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ARP', '032', 'Peso Argentino', NULL, FALSE, '1985-07');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ARS', '032', 'Argentine Peso', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ARY', '032', 'Peso', NULL, FALSE, '1989 to 1990');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ATS', '040', 'Schilling', NULL, FALSE, '2002-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('AUD', '036', 'Australian Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('AWG', '533', 'Aruban Florin', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('AYM', '945', 'Azerbaijan Manat', NULL, FALSE, '2005-10');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('AZM', '031', 'Azerbaijanian Manat', NULL, FALSE, '2005-12');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('AZN', '944', 'Azerbaijan Manat', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BAD', '070', 'Dinar', NULL, FALSE, '1998-07');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BAM', '977', 'Convertible Mark', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BBD', '052', 'Barbados Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BDT', '050', 'Taka', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BEC', '993', 'Convertible Franc', NULL, FALSE, '1990-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BEF', '056', 'Belgian Franc', NULL, FALSE, '2002-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BEL', '992', 'Financial Franc', NULL, FALSE, '1990-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BGJ', '100', 'Lev A/52', NULL, FALSE, '1989 to 1990');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BGK', '100', 'Lev A/62', NULL, FALSE, '1989 to 1990');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BGL', '100', 'Lev', NULL, FALSE, '2003-11');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BHD', '048', 'Bahraini Dinar', 3, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BIF', '108', 'Burundi Franc', 0, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BMD', '060', 'Bermudian Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BND', '096', 'Brunei Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BOB', '068', 'Boliviano', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BOP', '068', 'Peso boliviano', NULL, FALSE, '1987-02');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BOV', '984', 'Mvdol', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BRB', '076', 'Cruzeiro', NULL, FALSE, '1986-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BRC', '076', 'Cruzado', NULL, FALSE, '1989-02');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BRE', '076', 'Cruzeiro', NULL, FALSE, '1993-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BRL', '986', 'Brazilian Real', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BRN', '076', 'New Cruzado', NULL, FALSE, '1990-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BRR', '987', 'Cruzeiro Real', NULL, FALSE, '1994-07');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BSD', '044', 'Bahamian Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BTN', '064', 'Ngultrum', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BUK', '104', 'Kyat', NULL, FALSE, '1990-02');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BWP', '072', 'Pula', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BYB', '112', 'Belarusian Ruble', NULL, FALSE, '2001-01');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BYN', '933', 'Belarusian Ruble', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BYR', '974', 'Belarusian Ruble', NULL, FALSE, '2017-01');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('BZD', '084', 'Belize Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('CAD', '124', 'Canadian Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('CDF', '976', 'Congolese Franc', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('CHC', '948', 'WIR Franc (for electronic)', NULL, FALSE, '2004-11');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('CHE', '947', 'WIR Euro', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('CHF', '756', 'Swiss Franc', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('CHW', '948', 'WIR Franc', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('CLF', '990', 'Unidad de Fomento', 4, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('CLP', '152', 'Chilean Peso', 0, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('CNY', '156', 'Yuan Renminbi', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('COP', '170', 'Colombian Peso', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('COU', '970', 'Unidad de Valor Real', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('CRC', '188', 'Costa Rican Colon', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('CSD', '891', 'Serbian Dinar', NULL, FALSE, '2006-10');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('CSJ', '203', 'Krona A/53', NULL, FALSE, '1989 to 1990');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('CSK', '200', 'Koruna', NULL, FALSE, '1993-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('CUC', '931', 'Peso Convertible', NULL, FALSE, '2021-06');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('CUP', '192', 'Cuban Peso', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('CVE', '132', 'Cabo Verde Escudo', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('CYP', '196', 'Cyprus Pound', NULL, FALSE, '2008-01');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('CZK', '203', 'Czech Koruna', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('DDM', '278', 'Mark der DDR', NULL, FALSE, '1990-07 to 1990-09');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('DEM', '276', 'Deutsche Mark', NULL, FALSE, '2002-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('DJF', '262', 'Djibouti Franc', 0, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('DKK', '208', 'Danish Krone', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('DOP', '214', 'Dominican Peso', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('DZD', '012', 'Algerian Dinar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ECS', '218', 'Sucre', NULL, FALSE, '2000-09');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ECV', '983', 'Unidad de Valor Constante (UVC)', NULL, FALSE, '2000-09');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('EEK', '233', 'Kroon', NULL, FALSE, '2011-01');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('EGP', '818', 'Egyptian Pound', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ERN', '232', 'Nakfa', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ESA', '996', 'Spanish Peseta', NULL, FALSE, '1978 to 1981');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ESB', '995', '"A" Account (convertible Peseta Account)', NULL, FALSE, '1994-12');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ESP', '724', 'Spanish Peseta', NULL, FALSE, '2002-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ETB', '230', 'Ethiopian Birr', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('EUR', '978', 'Euro', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('FIM', '246', 'Markka', NULL, FALSE, '2002-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('FJD', '242', 'Fiji Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('FKP', '238', 'Falkland Islands Pound', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('FRF', '250', 'French Franc', NULL, FALSE, '2002-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('GBP', '826', 'Pound Sterling', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('GEK', '268', 'Georgian Coupon', NULL, FALSE, '1995-10');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('GEL', '981', 'Lari', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('GHC', '288', 'Cedi', NULL, FALSE, '2008-01');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('GHP', '939', 'Ghana Cedi', NULL, FALSE, '2007-06');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('GHS', '936', 'Ghana Cedi', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('GIP', '292', 'Gibraltar Pound', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('GMD', '270', 'Dalasi', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('GNE', '324', 'Syli', NULL, FALSE, '1989-12');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('GNF', '324', 'Guinean Franc', 0, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('GNS', '324', 'Syli', NULL, FALSE, '1986-02');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('GQE', '226', 'Ekwele', NULL, FALSE, '1986-06');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('GRD', '300', 'Drachma', NULL, FALSE, '2002-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('GTQ', '320', 'Quetzal', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('GWE', '624', 'Guinea Escudo', NULL, FALSE, '1978 to 1981');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('GWP', '624', 'Guinea-Bissau Peso', NULL, FALSE, '1997-05');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('GYD', '328', 'Guyana Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('HKD', '344', 'Hong Kong Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('HNL', '340', 'Lempira', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('HRD', '191', 'Croatian Dinar', NULL, FALSE, '1995-01');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('HRK', '191', 'Croatian Kuna', NULL, FALSE, '2015-06');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('HTG', '332', 'Gourde', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('HUF', '348', 'Forint', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('IDR', '360', 'Rupiah', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('IEP', '372', 'Irish Pound', NULL, FALSE, '2002-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ILP', '376', 'Pound', NULL, FALSE, '1978 to 1981');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ILR', '376', 'Old Shekel', NULL, FALSE, '1989 to 1990');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ILS', '376', 'New Israeli Sheqel', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('INR', '356', 'Indian Rupee', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('IQD', '368', 'Iraqi Dinar', 3, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('IRR', '364', 'Iranian Rial', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ISJ', '352', 'Old Krona', NULL, FALSE, '1989 to 1990');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ISK', '352', 'Iceland Krona', 0, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ITL', '380', 'Italian Lira', NULL, FALSE, '2002-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('JMD', '388', 'Jamaican Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('JOD', '400', 'Jordanian Dinar', 3, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('JPY', '392', 'Yen', 0, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('KES', '404', 'Kenyan Shilling', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('KGS', '417', 'Som', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('KHR', '116', 'Riel', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('KMF', '174', 'Comorian Franc', 0, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('KPW', '408', 'North Korean Won', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('KRW', '410', 'Won', 0, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('KWD', '414', 'Kuwaiti Dinar', 3, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('KYD', '136', 'Cayman Islands Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('KZT', '398', 'Tenge', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('LAJ', '418', 'Pathet Lao Kip', NULL, FALSE, '1979-12');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('LAK', '418', 'Lao Kip', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('LBP', '422', 'Lebanese Pound', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('LKR', '144', 'Sri Lanka Rupee', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('LRD', '430', 'Liberian Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('LSL', '426', 'Loti', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('LSM', '426', 'Loti', NULL, FALSE, '1985-05');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('LTL', '440', 'Lithuanian Litas', NULL, FALSE, '2014-12');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('LTT', '440', 'Talonas', NULL, FALSE, '1993-07');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('LUC', '989', 'Luxembourg Convertible Franc', NULL, FALSE, '1990-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('LUF', '442', 'Luxembourg Franc', NULL, FALSE, '2002-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('LUL', '988', 'Luxembourg Financial Franc', NULL, FALSE, '1990-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('LVL', '428', 'Latvian Lats', NULL, FALSE, '2014-01');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('LVR', '428', 'Latvian Ruble', NULL, FALSE, '1994-12');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('LYD', '434', 'Libyan Dinar', 3, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MAD', '504', 'Moroccan Dirham', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MDL', '498', 'Moldovan Leu', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MGA', '969', 'Malagasy Ariary', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MGF', '450', 'Malagasy Franc', NULL, FALSE, '2004-12');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MKD', '807', 'Denar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MLF', '466', 'Mali Franc', NULL, FALSE, '1984-11');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MMK', '104', 'Kyat', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MNT', '496', 'Tugrik', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MOP', '446', 'Pataca', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MRO', '478', 'Ouguiya', NULL, FALSE, '2017-12');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MRU', '929', 'Ouguiya', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MTL', '470', 'Maltese Lira', NULL, FALSE, '2008-01');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MTP', '470', 'Maltese Pound', NULL, FALSE, '1983-06');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MUR', '480', 'Mauritius Rupee', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MVQ', '462', 'Maldive Rupee', NULL, FALSE, '1989-12');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MVR', '462', 'Rufiyaa', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MWK', '454', 'Malawi Kwacha', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MXN', '484', 'Mexican Peso', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MXP', '484', 'Mexican Peso', NULL, FALSE, '1993-01');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MXV', '979', 'Mexican Unidad de Inversion (UDI)', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MYR', '458', 'Malaysian Ringgit', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MZE', '508', 'Mozambique Escudo', NULL, FALSE, '1978 to 1981');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MZM', '508', 'Mozambique Metical', NULL, FALSE, '2006-06');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('MZN', '943', 'Mozambique Metical', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('NAD', '516', 'Namibia Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('NGN', '566', 'Naira', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('NIC', '558', 'Cordoba', NULL, FALSE, '1990-10');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('NIO', '558', 'Cordoba Oro', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('NLG', '528', 'Netherlands Guilder', NULL, FALSE, '2002-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('NOK', '578', 'Norwegian Krone', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('NPR', '524', 'Nepalese Rupee', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('NZD', '554', 'New Zealand Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('OMR', '512', 'Rial Omani', 3, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('PAB', '590', 'Balboa', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('PEH', '604', 'Sol', NULL, FALSE, '1989 to 1990');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('PEI', '604', 'Inti', NULL, FALSE, '1991-07');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('PEN', '604', 'Sol', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('PES', '604', 'Sol', NULL, FALSE, '1986-02');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('PGK', '598', 'Kina', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('PHP', '608', 'Philippine Peso', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('PKR', '586', 'Pakistan Rupee', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('PLN', '985', 'Zloty', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('PLZ', '616', 'Zloty', NULL, FALSE, '1997-01');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('PTE', '620', 'Portuguese Escudo', NULL, FALSE, '2002-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('PYG', '600', 'Guarani', 0, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('QAR', '634', 'Qatari Rial', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('RHD', '716', 'Rhodesian Dollar', NULL, FALSE, '1978 to 1981');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ROK', '642', 'Leu A/52', NULL, FALSE, '1989 to 1990');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ROL', '642', 'Old Leu', NULL, FALSE, '2005-06');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('RON', '946', 'Romanian Leu', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('RSD', '941', 'Serbian Dinar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('RUB', '643', 'Russian Ruble', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('RUR', '810', 'Russian Ruble', NULL, FALSE, '1994-08');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('RWF', '646', 'Rwanda Franc', 0, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('SAR', '682', 'Saudi Riyal', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('SBD', '090', 'Solomon Islands Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('SCR', '690', 'Seychelles Rupee', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('SDD', '736', 'Sudanese Dinar', NULL, FALSE, '2007-07');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('SDG', '938', 'Sudanese Pound', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('SDP', '736', 'Sudanese Pound', NULL, FALSE, '1998-06');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('SEK', '752', 'Swedish Krona', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('SGD', '702', 'Singapore Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('SHP', '654', 'Saint Helena Pound', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('SIT', '705', 'Tolar', NULL, FALSE, '2007-01');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('SKK', '703', 'Slovak Koruna', NULL, FALSE, '2009-01');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('SLE', '925', 'Leone', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('SLL', '694', 'Leone', NULL, FALSE, '2023-12');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('SOS', '706', 'Somali Shilling', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('SRD', '968', 'Surinam Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('SRG', '740', 'Surinam Guilder', NULL, FALSE, '2003-12');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('SSP', '728', 'South Sudanese Pound', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('STD', '678', 'Dobra', NULL, FALSE, '2017-12');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('STN', '930', 'Dobra', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('SUR', '810', 'Rouble', NULL, FALSE, '1990-12');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('SVC', '222', 'El Salvador Colon', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('SYP', '760', 'Syrian Pound', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('SZL', '748', 'Lilangeni', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('THB', '764', 'Baht', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('TJR', '762', 'Tajik Ruble', NULL, FALSE, '2001-04');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('TJS', '972', 'Somoni', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('TMM', '795', 'Turkmenistan Manat', NULL, FALSE, '2009-01');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('TMT', '934', 'Turkmenistan New Manat', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('TND', '788', 'Tunisian Dinar', 3, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('TOP', '776', 'Pa’anga', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('TPE', '626', 'Timor Escudo', NULL, FALSE, '2002-11');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('TRL', '792', 'Old Turkish Lira', NULL, FALSE, '2005-12');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('TRY', '949', 'Turkish Lira', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('TTD', '780', 'Trinidad and Tobago Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('TWD', '901', 'New Taiwan Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('TZS', '834', 'Tanzanian Shilling', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('UAH', '980', 'Hryvnia', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('UAK', '804', 'Karbovanet', NULL, FALSE, '1996-09');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('UGS', '800', 'Uganda Shilling', NULL, FALSE, '1987-05');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('UGW', '800', 'Old Shilling', NULL, FALSE, '1989 to 1990');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('UGX', '800', 'Uganda Shilling', 0, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('USD', '840', 'US Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('USN', '997', 'US Dollar (Next day)', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('USS', '998', 'US Dollar (Same day)', NULL, FALSE, '2014-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('UYI', '940', 'Uruguay Peso en Unidades Indexadas (UI)', 0, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('UYN', '858', 'Old Uruguay Peso', NULL, FALSE, '1989-12');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('UYP', '858', 'Uruguayan Peso', NULL, FALSE, '1993-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('UYU', '858', 'Peso Uruguayo', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('UYW', '927', 'Unidad Previsional', 4, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('UZS', '860', 'Uzbekistan Sum', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('VEB', '862', 'Bolivar', NULL, FALSE, '2008-01');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('VED', '926', 'Bolívar Soberano', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('VEF', '937', 'Bolivar Fuerte', NULL, FALSE, '2011-12');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('VES', '928', 'Bolívar Soberano', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('VNC', '704', 'Old Dong', NULL, FALSE, '1989-1990');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('VND', '704', 'Dong', 0, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('VUV', '548', 'Vatu', 0, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('WST', '882', 'Tala', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XAD', '396', 'Arab Accounting Dinar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XAF', '950', 'CFA Franc BEAC', 0, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XAG', '961', 'Silver', NULL, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XAU', '959', 'Gold', NULL, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XBA', '955', 'Bond Markets Unit European Composite Unit (EURCO)', NULL, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XBB', '956', 'Bond Markets Unit European Monetary Unit (E.M.U.-6)', NULL, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XBC', '957', 'Bond Markets Unit European Unit of Account 9 (E.U.A.-9)', NULL, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XBD', '958', 'Bond Markets Unit European Unit of Account 17 (E.U.A.-17)', NULL, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XCD', '951', 'East Caribbean Dollar', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XCG', '532', 'Caribbean Guilder', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XDR', '960', 'SDR (Special Drawing Right)', NULL, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XEU', '954', 'European Currency Unit (E.C.U)', NULL, FALSE, '1999-01');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XFO', NULL, 'Gold-Franc', NULL, FALSE, '2006-10');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XFU', NULL, 'UIC-Franc', NULL, FALSE, '2013-11');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XOF', '952', 'CFA Franc BCEAO', 0, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XPD', '964', 'Palladium', NULL, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XPF', '953', 'CFP Franc', 0, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XPT', '962', 'Platinum', NULL, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XRE', NULL, 'RINET Funds Code', NULL, FALSE, '1999-11');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XSU', '994', 'Sucre', NULL, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XTS', '963', 'Codes specifically reserved for testing purposes', NULL, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XUA', '965', 'ADB Unit of Account', NULL, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('XXX', '999', 'The codes assigned for transactions where no currency is involved', NULL, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('YDD', '720', 'Yemeni Dinar', NULL, FALSE, '1991-09');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('YER', '886', 'Yemeni Rial', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('YUD', '890', 'New Yugoslavian Dinar', NULL, FALSE, '1990-01');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('YUM', '891', 'New Dinar', NULL, FALSE, '2003-07');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('YUN', '890', 'Yugoslavian Dinar', NULL, FALSE, '1995-11');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ZAL', '991', 'Financial Rand', NULL, FALSE, '1995-03');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ZAR', '710', 'Rand', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ZMK', '894', 'Zambian Kwacha', NULL, FALSE, '2012-12');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ZMW', '967', 'Zambian Kwacha', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ZRN', '180', 'New Zaire', NULL, FALSE, '1999-06');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ZRZ', '180', 'Zaire', NULL, FALSE, '1994-02');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ZWC', '716', 'Rhodesian Dollar', NULL, FALSE, '1989-12');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ZWD', '716', 'Zimbabwe Dollar (old)', NULL, FALSE, '2006-08');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ZWG', '924', 'Zimbabwe Gold', 2, TRUE, NULL);
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ZWL', '932', 'Zimbabwe Dollar', NULL, FALSE, '2024-09');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ZWN', '942', 'Zimbabwe Dollar (new)', NULL, FALSE, '2006-09');
INSERT INTO ccy_enum (code, numeric_code, name, minor_units, is_active, withdrawal_date) VALUES ('ZWR', '935', 'Zimbabwe Dollar', NULL, FALSE, '2009-06');

-- Demo tables: client, product, purchase
-- (HSQLDB supports INFORMATION_SCHEMA for introspection.)

DROP TABLE IF EXISTS purchase;
DROP TABLE IF EXISTS product;
DROP TABLE IF EXISTS client;

CREATE TABLE client (
  id       BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  email    VARCHAR(255) NOT NULL UNIQUE,
  name     VARCHAR(200) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE product (
  id        BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  sku       VARCHAR(64) NOT NULL UNIQUE,
  title     VARCHAR(255) NOT NULL,
  price_cents INTEGER NOT NULL CHECK (price_cents >= 0)
);

CREATE TABLE purchase (
  id          BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  client_id   BIGINT NOT NULL,
  product_id  BIGINT NOT NULL,
  qty         INTEGER NOT NULL CHECK (qty > 0),
  ordered_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  CONSTRAINT fk_purchase_client FOREIGN KEY (client_id) REFERENCES client(id),
  CONSTRAINT fk_purchase_product FOREIGN KEY (product_id) REFERENCES product(id)
);
