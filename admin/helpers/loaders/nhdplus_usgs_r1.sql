DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrlat;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipma;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm01;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm02;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm03;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm04;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm05;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm06;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm07;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm08;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm09;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm10;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm11;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm12;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrroma;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempma;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm01;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm02;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm03;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm04;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm05;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm06;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm07;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm08;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm09;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm10;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm11;
DROP TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm12;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrlat(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misslat     NUMERIC
   ,meanlat     NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrlat_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrlat_01i ON cipsrv_upload.r1_nhdplusincrlat(nhdplusid);
CREATE INDEX r1_nhdplusincrlat_02i ON cipsrv_upload.r1_nhdplusincrlat(hydroseq);
CREATE INDEX r1_nhdplusincrlat_03i ON cipsrv_upload.r1_nhdplusincrlat(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrlat TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrprecipma(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misspma     NUMERIC
   ,precipma    NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrprecipma_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrprecipma_01i ON cipsrv_upload.r1_nhdplusincrprecipma(nhdplusid);
CREATE INDEX r1_nhdplusincrprecipma_02i ON cipsrv_upload.r1_nhdplusincrprecipma(hydroseq);
CREATE INDEX r1_nhdplusincrprecipma_03i ON cipsrv_upload.r1_nhdplusincrprecipma(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrprecipma TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrprecipmm01(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misspmm01   NUMERIC
   ,precipmm01  NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrprecipmm01_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrprecipmm01_01i ON cipsrv_upload.r1_nhdplusincrprecipmm01(nhdplusid);
CREATE INDEX r1_nhdplusincrprecipmm01_02i ON cipsrv_upload.r1_nhdplusincrprecipmm01(hydroseq);
CREATE INDEX r1_nhdplusincrprecipmm01_03i ON cipsrv_upload.r1_nhdplusincrprecipmm01(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrprecipmm01 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrprecipmm02(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misspmm02   NUMERIC
   ,precipmm02  NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrprecipmm02_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrprecipmm02_01i ON cipsrv_upload.r1_nhdplusincrprecipmm02(nhdplusid);
CREATE INDEX r1_nhdplusincrprecipmm02_02i ON cipsrv_upload.r1_nhdplusincrprecipmm02(hydroseq);
CREATE INDEX r1_nhdplusincrprecipmm02_03i ON cipsrv_upload.r1_nhdplusincrprecipmm02(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrprecipmm02 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrprecipmm03(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misspmm03   NUMERIC
   ,precipmm03  NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrprecipmm03_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrprecipmm03_01i ON cipsrv_upload.r1_nhdplusincrprecipmm03(nhdplusid);
CREATE INDEX r1_nhdplusincrprecipmm03_02i ON cipsrv_upload.r1_nhdplusincrprecipmm03(hydroseq);
CREATE INDEX r1_nhdplusincrprecipmm03_03i ON cipsrv_upload.r1_nhdplusincrprecipmm03(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrprecipmm03 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrprecipmm04(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misspmm04   NUMERIC
   ,precipmm04  NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrprecipmm04_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrprecipmm04_01i ON cipsrv_upload.r1_nhdplusincrprecipmm04(nhdplusid);
CREATE INDEX r1_nhdplusincrprecipmm04_02i ON cipsrv_upload.r1_nhdplusincrprecipmm04(hydroseq);
CREATE INDEX r1_nhdplusincrprecipmm04_03i ON cipsrv_upload.r1_nhdplusincrprecipmm04(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrprecipmm04 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrprecipmm05(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misspmm05   NUMERIC
   ,precipmm05  NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrprecipmm05_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrprecipmm05_01i ON cipsrv_upload.r1_nhdplusincrprecipmm05(nhdplusid);
CREATE INDEX r1_nhdplusincrprecipmm05_02i ON cipsrv_upload.r1_nhdplusincrprecipmm05(hydroseq);
CREATE INDEX r1_nhdplusincrprecipmm05_03i ON cipsrv_upload.r1_nhdplusincrprecipmm05(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrprecipmm05 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrprecipmm06(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misspmm06   NUMERIC
   ,precipmm06  NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrprecipmm06_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrprecipmm06_01i ON cipsrv_upload.r1_nhdplusincrprecipmm06(nhdplusid);
CREATE INDEX r1_nhdplusincrprecipmm06_02i ON cipsrv_upload.r1_nhdplusincrprecipmm06(hydroseq);
CREATE INDEX r1_nhdplusincrprecipmm06_03i ON cipsrv_upload.r1_nhdplusincrprecipmm06(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrprecipmm06 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrprecipmm07(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misspmm07   NUMERIC
   ,precipmm07  NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrprecipmm07_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrprecipmm07_01i ON cipsrv_upload.r1_nhdplusincrprecipmm07(nhdplusid);
CREATE INDEX r1_nhdplusincrprecipmm07_02i ON cipsrv_upload.r1_nhdplusincrprecipmm07(hydroseq);
CREATE INDEX r1_nhdplusincrprecipmm07_03i ON cipsrv_upload.r1_nhdplusincrprecipmm07(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrprecipmm07 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrprecipmm08(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misspmm08   NUMERIC
   ,precipmm08  NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrprecipmm08_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrprecipmm08_01i ON cipsrv_upload.r1_nhdplusincrprecipmm08(nhdplusid);
CREATE INDEX r1_nhdplusincrprecipmm08_02i ON cipsrv_upload.r1_nhdplusincrprecipmm08(hydroseq);
CREATE INDEX r1_nhdplusincrprecipmm08_03i ON cipsrv_upload.r1_nhdplusincrprecipmm08(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrprecipmm08 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrprecipmm09(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misspmm09   NUMERIC
   ,precipmm09  NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrprecipmm09_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrprecipmm09_01i ON cipsrv_upload.r1_nhdplusincrprecipmm09(nhdplusid);
CREATE INDEX r1_nhdplusincrprecipmm09_02i ON cipsrv_upload.r1_nhdplusincrprecipmm09(hydroseq);
CREATE INDEX r1_nhdplusincrprecipmm09_03i ON cipsrv_upload.r1_nhdplusincrprecipmm09(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrprecipmm09 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrprecipmm10(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misspmm10   NUMERIC
   ,precipmm10  NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrprecipmm10_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrprecipmm10_01i ON cipsrv_upload.r1_nhdplusincrprecipmm10(nhdplusid);
CREATE INDEX r1_nhdplusincrprecipmm10_02i ON cipsrv_upload.r1_nhdplusincrprecipmm10(hydroseq);
CREATE INDEX r1_nhdplusincrprecipmm10_03i ON cipsrv_upload.r1_nhdplusincrprecipmm10(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrprecipmm10 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrprecipmm11(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misspmm11   NUMERIC
   ,precipmm11  NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrprecipmm11_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrprecipmm11_01i ON cipsrv_upload.r1_nhdplusincrprecipmm11(nhdplusid);
CREATE INDEX r1_nhdplusincrprecipmm11_02i ON cipsrv_upload.r1_nhdplusincrprecipmm11(hydroseq);
CREATE INDEX r1_nhdplusincrprecipmm11_03i ON cipsrv_upload.r1_nhdplusincrprecipmm11(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrprecipmm11 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrprecipmm12(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misspmm12   NUMERIC
   ,precipmm12  NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrprecipmm12_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrprecipmm12_01i ON cipsrv_upload.r1_nhdplusincrprecipmm12(nhdplusid);
CREATE INDEX r1_nhdplusincrprecipmm12_02i ON cipsrv_upload.r1_nhdplusincrprecipmm12(hydroseq);
CREATE INDEX r1_nhdplusincrprecipmm12_03i ON cipsrv_upload.r1_nhdplusincrprecipmm12(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrprecipmm12 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrroma(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,missrma     NUMERIC
   ,runoffma    NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrroma_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrroma_01i ON cipsrv_upload.r1_nhdplusincrroma(nhdplusid);
CREATE INDEX r1_nhdplusincrroma_02i ON cipsrv_upload.r1_nhdplusincrroma(hydroseq);
CREATE INDEX r1_nhdplusincrroma_03i ON cipsrv_upload.r1_nhdplusincrroma(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrroma TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrtempma(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misstma     NUMERIC
   ,tempma      NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrtempma_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrtempma_01i ON cipsrv_upload.r1_nhdplusincrtempma(nhdplusid);
CREATE INDEX r1_nhdplusincrtempma_02i ON cipsrv_upload.r1_nhdplusincrtempma(hydroseq);
CREATE INDEX r1_nhdplusincrtempma_03i ON cipsrv_upload.r1_nhdplusincrtempma(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrtempma TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrtempmm01(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misstmm01   NUMERIC
   ,tempmm01    NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrtempmm01_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrtempmm01_01i ON cipsrv_upload.r1_nhdplusincrtempmm01(nhdplusid);
CREATE INDEX r1_nhdplusincrtempmm01_02i ON cipsrv_upload.r1_nhdplusincrtempmm01(hydroseq);
CREATE INDEX r1_nhdplusincrtempmm01_03i ON cipsrv_upload.r1_nhdplusincrtempmm01(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrtempmm01 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrtempmm02(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misstmm02   NUMERIC
   ,tempmm02    NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrtempmm02_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrtempmm02_01i ON cipsrv_upload.r1_nhdplusincrtempmm02(nhdplusid);
CREATE INDEX r1_nhdplusincrtempmm02_02i ON cipsrv_upload.r1_nhdplusincrtempmm02(hydroseq);
CREATE INDEX r1_nhdplusincrtempmm02_03i ON cipsrv_upload.r1_nhdplusincrtempmm02(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrtempmm02 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrtempmm03(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misstmm03   NUMERIC
   ,tempmm03    NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrtempmm03_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrtempmm03_01i ON cipsrv_upload.r1_nhdplusincrtempmm03(nhdplusid);
CREATE INDEX r1_nhdplusincrtempmm03_02i ON cipsrv_upload.r1_nhdplusincrtempmm03(hydroseq);
CREATE INDEX r1_nhdplusincrtempmm03_03i ON cipsrv_upload.r1_nhdplusincrtempmm03(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrtempmm03 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrtempmm04(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misstmm04   NUMERIC
   ,tempmm04    NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrtempmm04_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrtempmm04_01i ON cipsrv_upload.r1_nhdplusincrtempmm04(nhdplusid);
CREATE INDEX r1_nhdplusincrtempmm04_02i ON cipsrv_upload.r1_nhdplusincrtempmm04(hydroseq);
CREATE INDEX r1_nhdplusincrtempmm04_03i ON cipsrv_upload.r1_nhdplusincrtempmm04(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrtempmm04 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrtempmm05(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misstmm05   NUMERIC
   ,tempmm05    NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrtempmm05_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrtempmm05_01i ON cipsrv_upload.r1_nhdplusincrtempmm05(nhdplusid);
CREATE INDEX r1_nhdplusincrtempmm05_02i ON cipsrv_upload.r1_nhdplusincrtempmm05(hydroseq);
CREATE INDEX r1_nhdplusincrtempmm05_03i ON cipsrv_upload.r1_nhdplusincrtempmm05(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrtempmm05 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrtempmm06(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misstmm06   NUMERIC
   ,tempmm06    NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrtempmm06_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrtempmm06_01i ON cipsrv_upload.r1_nhdplusincrtempmm06(nhdplusid);
CREATE INDEX r1_nhdplusincrtempmm06_02i ON cipsrv_upload.r1_nhdplusincrtempmm06(hydroseq);
CREATE INDEX r1_nhdplusincrtempmm06_03i ON cipsrv_upload.r1_nhdplusincrtempmm06(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrtempmm06 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrtempmm07(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misstmm07   NUMERIC
   ,tempmm07    NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrtempmm07_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrtempmm07_01i ON cipsrv_upload.r1_nhdplusincrtempmm07(nhdplusid);
CREATE INDEX r1_nhdplusincrtempmm07_02i ON cipsrv_upload.r1_nhdplusincrtempmm07(hydroseq);
CREATE INDEX r1_nhdplusincrtempmm07_03i ON cipsrv_upload.r1_nhdplusincrtempmm07(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrtempmm07 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrtempmm08(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misstmm08   NUMERIC
   ,tempmm08    NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrtempmm08_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrtempmm08_01i ON cipsrv_upload.r1_nhdplusincrtempmm08(nhdplusid);
CREATE INDEX r1_nhdplusincrtempmm08_02i ON cipsrv_upload.r1_nhdplusincrtempmm08(hydroseq);
CREATE INDEX r1_nhdplusincrtempmm08_03i ON cipsrv_upload.r1_nhdplusincrtempmm08(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrtempmm08 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrtempmm09(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misstmm09   NUMERIC
   ,tempmm09    NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrtempmm09_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrtempmm09_01i ON cipsrv_upload.r1_nhdplusincrtempmm09(nhdplusid);
CREATE INDEX r1_nhdplusincrtempmm09_02i ON cipsrv_upload.r1_nhdplusincrtempmm09(hydroseq);
CREATE INDEX r1_nhdplusincrtempmm09_03i ON cipsrv_upload.r1_nhdplusincrtempmm09(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrtempmm09 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrtempmm10(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misstmm10   NUMERIC
   ,tempmm10    NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrtempmm10_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrtempmm10_01i ON cipsrv_upload.r1_nhdplusincrtempmm10(nhdplusid);
CREATE INDEX r1_nhdplusincrtempmm10_02i ON cipsrv_upload.r1_nhdplusincrtempmm10(hydroseq);
CREATE INDEX r1_nhdplusincrtempmm10_03i ON cipsrv_upload.r1_nhdplusincrtempmm10(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrtempmm10 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrtempmm11(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misstmm11   NUMERIC
   ,tempmm11    NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrtempmm11_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrtempmm11_01i ON cipsrv_upload.r1_nhdplusincrtempmm11(nhdplusid);
CREATE INDEX r1_nhdplusincrtempmm11_02i ON cipsrv_upload.r1_nhdplusincrtempmm11(hydroseq);
CREATE INDEX r1_nhdplusincrtempmm11_03i ON cipsrv_upload.r1_nhdplusincrtempmm11(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrtempmm11 TO PUBLIC;


CREATE TABLE IF NOT EXISTS cipsrv_upload.r1_nhdplusincrtempmm12(
    objectid    INTEGER NOT NULL 
   ,nhdplusid   BIGINT  NOT NULL
   ,misstmm12   NUMERIC
   ,tempmm12    NUMERIC
   ,hydroseq    BIGINT
   ,vpuid       VARCHAR(8)
   ,CONSTRAINT r1_nhdplusincrtempmm12_u01 UNIQUE (objectid)
);
CREATE INDEX r1_nhdplusincrtempmm12_01i ON cipsrv_upload.r1_nhdplusincrtempmm12(nhdplusid);
CREATE INDEX r1_nhdplusincrtempmm12_02i ON cipsrv_upload.r1_nhdplusincrtempmm12(hydroseq);
CREATE INDEX r1_nhdplusincrtempmm12_03i ON cipsrv_upload.r1_nhdplusincrtempmm12(vpuid);
GRANT SELECT ON TABLE cipsrv_upload.r1_nhdplusincrtempmm12 TO PUBLIC;

CREATE OR REPLACE PROCEDURE cipsrv_upload.load_r1()
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE 
   tbls VARCHAR[];
   cols VARCHAR[];
   vpus VARCHAR[];
   idx  INTEGER;
   vpu  VARCHAR;
   str  VARCHAR;
   
BEGIN

   tbls := ARRAY[
      'nhdplusincrlat',
      'nhdplusincrprecipma',
      'nhdplusincrprecipmm01',
      'nhdplusincrprecipmm02',
      'nhdplusincrprecipmm03',
      'nhdplusincrprecipmm04',
      'nhdplusincrprecipmm05',
      'nhdplusincrprecipmm06',
      'nhdplusincrprecipmm07',
      'nhdplusincrprecipmm08',
      'nhdplusincrprecipmm09',
      'nhdplusincrprecipmm10',
      'nhdplusincrprecipmm11',
      'nhdplusincrprecipmm12',
      'nhdplusincrroma',
      'nhdplusincrtempma',
      'nhdplusincrtempmm01',
      'nhdplusincrtempmm02',
      'nhdplusincrtempmm03',
      'nhdplusincrtempmm04',
      'nhdplusincrtempmm05',
      'nhdplusincrtempmm06',
      'nhdplusincrtempmm07',
      'nhdplusincrtempmm08',
      'nhdplusincrtempmm09',
      'nhdplusincrtempmm10',
      'nhdplusincrtempmm11',
      'nhdplusincrtempmm12'
   ];
   
   cols := ARRAY[
      'a.nhdplusid, a.misslat,   a.meanlat,    a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misspma,   a.precipma,   a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misspmm01, a.precipmm01, a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misspmm02, a.precipmm02, a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misspmm03, a.precipmm03, a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misspmm04, a.precipmm04, a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misspmm05, a.precipmm05, a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misspmm06, a.precipmm06, a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misspmm07, a.precipmm07, a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misspmm08, a.precipmm08, a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misspmm09, a.precipmm09, a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misspmm10, a.precipmm10, a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misspmm11, a.precipmm11, a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misspmm12, a.precipmm12, a.hydroseq, a.vpuid',
      'a.nhdplusid, a.missrma,   a.runoffma,   a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misstma,   a.tempma,     a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misstmm01, a.tempmm01,   a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misstmm02, a.tempmm02,   a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misstmm03, a.tempmm03,   a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misstmm04, a.tempmm04,   a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misstmm05, a.tempmm05,   a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misstmm06, a.tempmm06,   a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misstmm07, a.tempmm07,   a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misstmm08, a.tempvmm08,  a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misstmm09, a.tempmm09,   a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misstmm10, a.tempmm10,   a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misstmm11, a.tempmm11,   a.hydroseq, a.vpuid',
      'a.nhdplusid, a.misstmm12, a.tempmm12,   a.hydroseq, a.vpuid'
   ];

   vpus := ARRAY[
      '0101',
      '0102',
      '0103',
      '0104',
      '0105',
      '0106',
      '0107',
      '0108',
      '0109',
      '0110',    
      '0202',
      '0203',
      '0204',
      '0205',
      '0206',
      '0207',
      '0208',
      '0301',
      '0302',
      '0303',
      '0304',
      '0305',
      '0306',
      '0307',
      '0308',
      '0309',
      '0310',
      '0311',
      '0312',
      '0313',
      '0314',
      '0315',
      '0316',
      '0317',
      '0318',  
      '0401',
      '0402',
      '0403',
      '0404',
      '0405',
      '0406',
      '0407',
      '0408',
      '0409',
      '0410',
      '0411',
      '0412',
      '0413',
      '0414',
      '0416',
      '0417',
      '0418',
      '0418i',
      '0419',
      '0419i',
      '0420',
      '0421',
      '0422',
      '0423',
      '0424',
      '0424i',
      '0425',
      '0426',
      '0426i',
      '0427',
      '0428',
      '0428i',
      '0429',
      '0430',
      '0431',
      '0432',
      '0433',
      '0501',
      '0502',
      '0503',
      '0504',
      '0505',
      '0506',
      '0507',
      '0508',
      '0509',
      '0510',
      '0511',
      '0512',
      '0513',
      '0514',
      '0601',
      '0602',
      '0603',
      '0604',
      '0701',
      '0702',
      '0703',
      '0704',
      '0705',
      '0706',
      '0707',
      '0708',
      '0709',
      '0710',
      '0711',
      '0712',
      '0713',
      '0714',
      '0801',
      '0802',
      '0803',
      '0804',
      '0805',
      '0806',
      '0807',
      '0808',
      '0809',
      '0901',
      '0902',
      '0903',
      '0904',
      '1002',
      '1003',
      '1004',
      '1005',
      '1006',
      '1007',
      '1008',
      '1009',
      '1010',
      '1011',
      '1012',
      '1013',
      '1014',
      '1015',
      '1016',
      '1017',
      '1018',
      '1019',
      '1020',
      '1021',
      '1022',
      '1023',
      '1024',
      '1025',
      '1026',
      '1027',
      '1028',
      '1029',
      '1030',
      '1101',
      '1102',
      '1103',
      '1104',
      '1105',
      '1106',
      '1107',
      '1108',
      '1109',
      '1110',
      '1111',
      '1112',
      '1113',
      '1114',
      '1201',
      '1202',
      '1203',
      '1204',
      '1205',
      '1206',
      '1207',
      '1208',
      '1209',
      '1210',
      '1211',
      '1301',
      '1302',
      '1303',
      '1304',
      '1305',
      '1306',
      '1307',
      '1308',
      '1309',
      '1401',
      '1402',
      '1403',
      '1404',
      '1405',
      '1406',
      '1407',
      '1408',
      '1501',
      '1502',
      '1503',
      '1504',
      '1505',
      '1506',
      '1507',
      '1508',
      '1601',
      '1602',
      '1603',
      '1604',
      '1605',
      '1606',
      '1701',
      '1702',
      '1703',
      '1704',
      '1705',
      '1706',
      '1707',
      '1708',
      '1709',
      '1710',
      '1711',
      '1712',
      '1801',
      '1802',
      '1803',
      '1804',
      '1805',
      '1806',
      '1807',
      '1808',
      '1809',
      '1810',
      '19020101',
      '19020102',
      '19020103',
      '19020104',
      '19020201',
      '19020202',
      '19020203',
      '19020301',
      '19020302',
      '19020401',
      '19020402',
      '19020501',
      '19020502',
      '19020503',
      '19020504',
      '19020505',
      '19020601',
      '19020602',
      '19020800',
      '19050401',
      '19060102',
      '19060501',
      '19060502',
      '19060504',
      '19070402',
      '19080301',
      '19080305',
      '2001',
      '2002',
      '2003',
      '2004',
      '2005',
      '2006',
      '2007',
      '2008',
      '2101',
      '2102',
      '2201',
      '2202',
      '2203'
   ];
   
    FOREACH vpu IN ARRAY vpus
    LOOP
    
       FOR idx IN 1 .. ARRAY_UPPER(tbls,1)
       LOOP
          str := 'INSERT INTO cipsrv_upload.r1_' || tbls[idx] || ' '
              || 'SELECT '
              || 'NEXTVAL(''cipsrv_upload.' || tbls[idx] || '_seq''), ' || cols[idx] || ' '
              || 'FROM cipsrv_upload.r1_vpu' || vpu || '_' || tbls[idx] || ' a ';
          --RAISE WARNING '%',str;
          
          EXECUTE str;
    
       END LOOP;
       
       COMMIT;
    
    END LOOP;

END 
$BODY$;

CALL cipsrv_upload.load_r1();

DELETE FROM cipsrv_upload.r1_nhdplusincrlat        a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipma   a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm01 a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm02 a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm03 a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm04 a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm05 a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm06 a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm07 a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm08 a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm09 a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm10 a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm11 a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm12 a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrroma       a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempma     a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm01   a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm02   a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm03   a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm04   a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm05   a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm06   a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm07   a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm08   a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm09   a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm10   a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm11   a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm12   a WHERE NOT EXISTS (SELECT 1 FROM cipsrv_nhdplus_h.networknhdflowline b WHERE b.nhdplusid = a.nhdplusid);


DELETE FROM cipsrv_upload.r1_nhdplusincrlat        a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrlat        b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipma   a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrprecipma   b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm01 a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrprecipmm01 b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm02 a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrprecipmm02 b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm03 a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrprecipmm03 b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm04 a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrprecipmm04 b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm05 a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrprecipmm05 b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm06 a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrprecipmm06 b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm07 a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrprecipmm07 b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm08 a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrprecipmm08 b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm09 a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrprecipmm09 b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm10 a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrprecipmm10 b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm11 a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrprecipmm11 b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrprecipmm12 a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrprecipmm12 b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrroma       a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrroma       b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempma     a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrtempma     b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm01   a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrtempmm01   b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm02   a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrtempmm02   b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm03   a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrtempmm03   b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm04   a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrtempmm04   b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm05   a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrtempmm05   b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm06   a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrtempmm06   b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm07   a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrtempmm07   b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm08   a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrtempmm08   b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm09   a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrtempmm09   b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm10   a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrtempmm10   b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm11   a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrtempmm11   b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);
DELETE FROM cipsrv_upload.r1_nhdplusincrtempmm12   a WHERE a.ctid <> (SELECT min(b.ctid) FROM cipsrv_upload.r1_nhdplusincrtempmm12   b WHERE a.nhdplusid = b.nhdplusid and a.vpuid = b.vpuid and a.hydroseq = b.hydroseq);

DROP INDEX cipsrv_upload.r1_nhdplusincrlat_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipma_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm01_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm02_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm03_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm04_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm05_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm06_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm07_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm08_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm09_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm10_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm11_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm12_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrroma_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempma_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm01_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm02_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm03_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm04_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm05_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm06_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm07_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm08_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm09_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm10_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm11_01i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm12_01i;

ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrlat        ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipma   ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm01 ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm02 ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm03 ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm04 ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm05 ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm06 ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm07 ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm08 ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm09 ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm10 ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm11 ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm12 ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrroma       ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempma     ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm01   ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm02   ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm03   ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm04   ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm05   ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm06   ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm07   ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm08   ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm09   ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm10   ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm11   ALTER COLUMN hydroseq SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm12   ALTER COLUMN hydroseq SET NOT NULL;

ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrlat        ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipma   ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm01 ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm02 ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm03 ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm04 ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm05 ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm06 ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm07 ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm08 ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm09 ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm10 ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm11 ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm12 ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrroma       ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempma     ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm01   ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm02   ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm03   ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm04   ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm05   ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm06   ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm07   ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm08   ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm09   ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm10   ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm11   ALTER COLUMN vpuid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm12   ALTER COLUMN vpuid SET NOT NULL;

ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrlat        ADD CONSTRAINT nhdplusincrlat_pk        PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipma   ADD CONSTRAINT nhdplusincrprecipma_pk   PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm01 ADD CONSTRAINT nhdplusincrprecipmm01_pk PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm02 ADD CONSTRAINT nhdplusincrprecipmm02_pk PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm03 ADD CONSTRAINT nhdplusincrprecipmm03_pk PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm04 ADD CONSTRAINT nhdplusincrprecipmm04_pk PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm05 ADD CONSTRAINT nhdplusincrprecipmm05_pk PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm06 ADD CONSTRAINT nhdplusincrprecipmm06_pk PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm07 ADD CONSTRAINT nhdplusincrprecipmm07_pk PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm08 ADD CONSTRAINT nhdplusincrprecipmm08_pk PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm09 ADD CONSTRAINT nhdplusincrprecipmm09_pk PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm10 ADD CONSTRAINT nhdplusincrprecipmm10_pk PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm11 ADD CONSTRAINT nhdplusincrprecipmm11_pk PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm12 ADD CONSTRAINT nhdplusincrprecipmm12_pk PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrroma       ADD CONSTRAINT nhdplusincrroma_pk       PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempma     ADD CONSTRAINT nhdplusincrtempma_pk     PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm01   ADD CONSTRAINT nhdplusincrtempmm01_pk   PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm02   ADD CONSTRAINT nhdplusincrtempmm02_pk   PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm03   ADD CONSTRAINT nhdplusincrtempmm03_pk   PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm04   ADD CONSTRAINT nhdplusincrtempmm04_pk   PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm05   ADD CONSTRAINT nhdplusincrtempmm05_pk   PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm06   ADD CONSTRAINT nhdplusincrtempmm06_pk   PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm07   ADD CONSTRAINT nhdplusincrtempmm07_pk   PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm08   ADD CONSTRAINT nhdplusincrtempmm08_pk   PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm09   ADD CONSTRAINT nhdplusincrtempmm09_pk   PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm10   ADD CONSTRAINT nhdplusincrtempmm10_pk   PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm11   ADD CONSTRAINT nhdplusincrtempmm11_pk   PRIMARY KEY (nhdplusid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm12   ADD CONSTRAINT nhdplusincrtempmm12_pk   PRIMARY KEY (nhdplusid);

DROP INDEX cipsrv_upload.r1_nhdplusincrlat_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipma_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm01_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm02_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm03_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm04_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm05_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm06_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm07_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm08_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm09_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm10_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm11_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrprecipmm12_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrroma_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempma_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm01_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm02_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm03_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm04_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm05_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm06_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm07_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm08_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm09_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm10_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm11_02i;
DROP INDEX cipsrv_upload.r1_nhdplusincrtempmm12_02i;

ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrlat        ADD CONSTRAINT nhdplusincrlat_u02        UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipma   ADD CONSTRAINT nhdplusincrprecipma_u02   UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm01 ADD CONSTRAINT nhdplusincrprecipmm01_u02 UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm02 ADD CONSTRAINT nhdplusincrprecipmm02_u02 UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm03 ADD CONSTRAINT nhdplusincrprecipmm03_u02 UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm04 ADD CONSTRAINT nhdplusincrprecipmm04_u02 UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm05 ADD CONSTRAINT nhdplusincrprecipmm05_u02 UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm06 ADD CONSTRAINT nhdplusincrprecipmm06_u02 UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm07 ADD CONSTRAINT nhdplusincrprecipmm07_u02 UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm08 ADD CONSTRAINT nhdplusincrprecipmm08_u02 UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm09 ADD CONSTRAINT nhdplusincrprecipmm09_u02 UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm10 ADD CONSTRAINT nhdplusincrprecipmm10_u02 UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm11 ADD CONSTRAINT nhdplusincrprecipmm11_u02 UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm12 ADD CONSTRAINT nhdplusincrprecipmm12_u02 UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrroma       ADD CONSTRAINT nhdplusincrroma_u02       UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempma     ADD CONSTRAINT nhdplusincrtempma_u02     UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm01   ADD CONSTRAINT nhdplusincrtempmm01_u02   UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm02   ADD CONSTRAINT nhdplusincrtempmm02_u02   UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm03   ADD CONSTRAINT nhdplusincrtempmm03_u02   UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm04   ADD CONSTRAINT nhdplusincrtempmm04_u02   UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm05   ADD CONSTRAINT nhdplusincrtempmm05_u02   UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm06   ADD CONSTRAINT nhdplusincrtempmm06_u02   UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm07   ADD CONSTRAINT nhdplusincrtempmm07_u02   UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm08   ADD CONSTRAINT nhdplusincrtempmm08_u02   UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm09   ADD CONSTRAINT nhdplusincrtempmm09_u02   UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm10   ADD CONSTRAINT nhdplusincrtempmm10_u02   UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm11   ADD CONSTRAINT nhdplusincrtempmm11_u02   UNIQUE (hydroseq);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm12   ADD CONSTRAINT nhdplusincrtempmm12_u02   UNIQUE (hydroseq);

ALTER TABLE cipsrv_upload.r1_nhdplusincrlat        RENAME CONSTRAINT r1_nhdplusincrlat_u01        TO nhdplusincrlat_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrprecipma   RENAME CONSTRAINT r1_nhdplusincrprecipma_u01   TO nhdplusincrprecipma_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrprecipmm01 RENAME CONSTRAINT r1_nhdplusincrprecipmm01_u01 TO nhdplusincrprecipmm01_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrprecipmm02 RENAME CONSTRAINT r1_nhdplusincrprecipmm02_u01 TO nhdplusincrprecipmm02_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrprecipmm03 RENAME CONSTRAINT r1_nhdplusincrprecipmm03_u01 TO nhdplusincrprecipmm03_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrprecipmm04 RENAME CONSTRAINT r1_nhdplusincrprecipmm04_u01 TO nhdplusincrprecipmm04_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrprecipmm05 RENAME CONSTRAINT r1_nhdplusincrprecipmm05_u01 TO nhdplusincrprecipmm05_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrprecipmm06 RENAME CONSTRAINT r1_nhdplusincrprecipmm06_u01 TO nhdplusincrprecipmm06_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrprecipmm07 RENAME CONSTRAINT r1_nhdplusincrprecipmm07_u01 TO nhdplusincrprecipmm07_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrprecipmm08 RENAME CONSTRAINT r1_nhdplusincrprecipmm08_u01 TO nhdplusincrprecipmm08_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrprecipmm09 RENAME CONSTRAINT r1_nhdplusincrprecipmm09_u01 TO nhdplusincrprecipmm09_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrprecipmm10 RENAME CONSTRAINT r1_nhdplusincrprecipmm10_u01 TO nhdplusincrprecipmm10_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrprecipmm11 RENAME CONSTRAINT r1_nhdplusincrprecipmm11_u01 TO nhdplusincrprecipmm11_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrprecipmm12 RENAME CONSTRAINT r1_nhdplusincrprecipmm12_u01 TO nhdplusincrprecipmm12_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrroma       RENAME CONSTRAINT r1_nhdplusincrroma_u01       TO nhdplusincrroma_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrtempma     RENAME CONSTRAINT r1_nhdplusincrtempma_u01     TO nhdplusincrtempma_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrtempmm01   RENAME CONSTRAINT r1_nhdplusincrtempmm01_u01   TO nhdplusincrtempmm01_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrtempmm02   RENAME CONSTRAINT r1_nhdplusincrtempmm02_u01   TO nhdplusincrtempmm02_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrtempmm03   RENAME CONSTRAINT r1_nhdplusincrtempmm03_u01   TO nhdplusincrtempmm03_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrtempmm04   RENAME CONSTRAINT r1_nhdplusincrtempmm04_u01   TO nhdplusincrtempmm04_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrtempmm05   RENAME CONSTRAINT r1_nhdplusincrtempmm05_u01   TO nhdplusincrtempmm05_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrtempmm06   RENAME CONSTRAINT r1_nhdplusincrtempmm06_u01   TO nhdplusincrtempmm06_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrtempmm07   RENAME CONSTRAINT r1_nhdplusincrtempmm07_u01   TO nhdplusincrtempmm07_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrtempmm08   RENAME CONSTRAINT r1_nhdplusincrtempmm08_u01   TO nhdplusincrtempmm08_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrtempmm09   RENAME CONSTRAINT r1_nhdplusincrtempmm09_u01   TO nhdplusincrtempmm09_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrtempmm10   RENAME CONSTRAINT r1_nhdplusincrtempmm10_u01   TO nhdplusincrtempmm10_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrtempmm11   RENAME CONSTRAINT r1_nhdplusincrtempmm11_u01   TO nhdplusincrtempmm11_u01;
ALTER TABLE cipsrv_upload.r1_nhdplusincrtempmm12   RENAME CONSTRAINT r1_nhdplusincrtempmm12_u01   TO nhdplusincrtempmm12_u01;

ALTER INDEX cipsrv_upload.r1_nhdplusincrlat_03i        RENAME TO nhdplusincrlat_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrprecipma_03i   RENAME TO nhdplusincrprecipma_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrprecipmm01_03i RENAME TO nhdplusincrprecipmm01_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrprecipmm02_03i RENAME TO nhdplusincrprecipmm02_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrprecipmm03_03i RENAME TO nhdplusincrprecipmm03_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrprecipmm04_03i RENAME TO nhdplusincrprecipmm04_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrprecipmm05_03i RENAME TO nhdplusincrprecipmm05_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrprecipmm06_03i RENAME TO nhdplusincrprecipmm06_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrprecipmm07_03i RENAME TO nhdplusincrprecipmm07_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrprecipmm08_03i RENAME TO nhdplusincrprecipmm08_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrprecipmm09_03i RENAME TO nhdplusincrprecipmm09_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrprecipmm10_03i RENAME TO nhdplusincrprecipmm10_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrprecipmm11_03i RENAME TO nhdplusincrprecipmm11_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrprecipmm12_03i RENAME TO nhdplusincrprecipmm12_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrroma_03i       RENAME TO nhdplusincrroma_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrtempma_03i     RENAME TO nhdplusincrtempma_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrtempmm01_03i   RENAME TO nhdplusincrtempmm01_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrtempmm02_03i   RENAME TO nhdplusincrtempmm02_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrtempmm03_03i   RENAME TO nhdplusincrtempmm03_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrtempmm04_03i   RENAME TO nhdplusincrtempmm04_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrtempmm05_03i   RENAME TO nhdplusincrtempmm05_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrtempmm06_03i   RENAME TO nhdplusincrtempmm06_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrtempmm07_03i   RENAME TO nhdplusincrtempmm07_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrtempmm08_03i   RENAME TO nhdplusincrtempmm08_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrtempmm09_03i   RENAME TO nhdplusincrtempmm09_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrtempmm10_03i   RENAME TO nhdplusincrtempmm10_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrtempmm11_03i   RENAME TO nhdplusincrtempmm11_01i;
ALTER INDEX cipsrv_upload.r1_nhdplusincrtempmm12_03i   RENAME TO nhdplusincrtempmm12_01i;

ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrlat        ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipma   ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm01 ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm02 ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm03 ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm04 ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm05 ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm06 ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm07 ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm08 ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm09 ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm10 ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm11 ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm12 ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrroma       ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempma     ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm01   ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm02   ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm03   ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm04   ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm05   ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm06   ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm07   ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm08   ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm09   ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm10   ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm11   ADD COLUMN globalid character varying(40);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm12   ADD COLUMN globalid character varying(40);

UPDATE cipsrv_upload.r1_nhdplusincrlat        SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrprecipma   SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrprecipmm01 SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrprecipmm02 SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrprecipmm03 SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrprecipmm04 SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrprecipmm05 SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrprecipmm06 SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrprecipmm07 SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrprecipmm08 SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrprecipmm09 SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrprecipmm10 SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrprecipmm11 SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrprecipmm12 SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrroma       SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrtempma     SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrtempmm01   SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrtempmm02   SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrtempmm03   SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrtempmm04   SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrtempmm05   SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrtempmm06   SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrtempmm07   SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrtempmm08   SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrtempmm09   SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrtempmm10   SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrtempmm11   SET globalid = '{' || uuid_generate_v1() || '}';
UPDATE cipsrv_upload.r1_nhdplusincrtempmm12   SET globalid = '{' || uuid_generate_v1() || '}';

ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrlat        ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipma   ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm01 ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm02 ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm03 ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm04 ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm05 ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm06 ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm07 ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm08 ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm09 ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm10 ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm11 ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm12 ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrroma       ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempma     ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm01   ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm02   ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm03   ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm04   ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm05   ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm06   ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm07   ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm08   ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm09   ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm10   ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm11   ALTER COLUMN globalid SET NOT NULL;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm12   ALTER COLUMN globalid SET NOT NULL;

ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrlat        ADD CONSTRAINT nhdplusincrlat_u03        UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipma   ADD CONSTRAINT nhdplusincrprecipma_u03   UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm01 ADD CONSTRAINT nhdplusincrprecipmm01_u03 UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm02 ADD CONSTRAINT nhdplusincrprecipmm02_u03 UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm03 ADD CONSTRAINT nhdplusincrprecipmm03_u03 UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm04 ADD CONSTRAINT nhdplusincrprecipmm04_u03 UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm05 ADD CONSTRAINT nhdplusincrprecipmm05_u03 UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm06 ADD CONSTRAINT nhdplusincrprecipmm06_u03 UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm07 ADD CONSTRAINT nhdplusincrprecipmm07_u03 UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm08 ADD CONSTRAINT nhdplusincrprecipmm08_u03 UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm09 ADD CONSTRAINT nhdplusincrprecipmm09_u03 UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm10 ADD CONSTRAINT nhdplusincrprecipmm10_u03 UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm11 ADD CONSTRAINT nhdplusincrprecipmm11_u03 UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm12 ADD CONSTRAINT nhdplusincrprecipmm12_u03 UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrroma       ADD CONSTRAINT nhdplusincrroma_u03       UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempma     ADD CONSTRAINT nhdplusincrtempma_u03     UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm01   ADD CONSTRAINT nhdplusincrtempmm01_u03   UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm02   ADD CONSTRAINT nhdplusincrtempmm02_u03   UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm03   ADD CONSTRAINT nhdplusincrtempmm03_u03   UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm04   ADD CONSTRAINT nhdplusincrtempmm04_u03   UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm05   ADD CONSTRAINT nhdplusincrtempmm05_u03   UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm06   ADD CONSTRAINT nhdplusincrtempmm06_u03   UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm07   ADD CONSTRAINT nhdplusincrtempmm07_u03   UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm08   ADD CONSTRAINT nhdplusincrtempmm08_u03   UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm09   ADD CONSTRAINT nhdplusincrtempmm09_u03   UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm10   ADD CONSTRAINT nhdplusincrtempmm10_u03   UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm11   ADD CONSTRAINT nhdplusincrtempmm11_u03   UNIQUE (globalid);
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm12   ADD CONSTRAINT nhdplusincrtempmm12_u03   UNIQUE (globalid);

ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrlat        RENAME TO nhdplusincrlat;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipma   RENAME TO nhdplusincrprecipma;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm01 RENAME TO nhdplusincrprecipmm01;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm02 RENAME TO nhdplusincrprecipmm02;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm03 RENAME TO nhdplusincrprecipmm03;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm04 RENAME TO nhdplusincrprecipmm04;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm05 RENAME TO nhdplusincrprecipmm05;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm06 RENAME TO nhdplusincrprecipmm06;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm07 RENAME TO nhdplusincrprecipmm07;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm08 RENAME TO nhdplusincrprecipmm08;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm09 RENAME TO nhdplusincrprecipmm09;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm10 RENAME TO nhdplusincrprecipmm10;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm11 RENAME TO nhdplusincrprecipmm11;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrprecipmm12 RENAME TO nhdplusincrprecipmm12;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrroma       RENAME TO nhdplusincrroma;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempma     RENAME TO nhdplusincrtempma;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm01   RENAME TO nhdplusincrtempmm01;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm02   RENAME TO nhdplusincrtempmm02;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm03   RENAME TO nhdplusincrtempmm03;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm04   RENAME TO nhdplusincrtempmm04;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm05   RENAME TO nhdplusincrtempmm05;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm06   RENAME TO nhdplusincrtempmm06;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm07   RENAME TO nhdplusincrtempmm07;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm08   RENAME TO nhdplusincrtempmm08;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm09   RENAME TO nhdplusincrtempmm09;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm10   RENAME TO nhdplusincrtempmm10;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm11   RENAME TO nhdplusincrtempmm11;
ALTER TABLE IF EXISTS cipsrv_upload.r1_nhdplusincrtempmm12   RENAME TO nhdplusincrtempmm12;

ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrlat        SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrprecipma   SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrprecipmm01 SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrprecipmm02 SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrprecipmm03 SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrprecipmm04 SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrprecipmm05 SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrprecipmm06 SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrprecipmm07 SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrprecipmm08 SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrprecipmm09 SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrprecipmm10 SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrprecipmm11 SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrprecipmm12 SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrroma       SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrtempma     SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrtempmm01   SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrtempmm02   SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrtempmm03   SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrtempmm04   SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrtempmm05   SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrtempmm06   SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrtempmm07   SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrtempmm08   SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrtempmm09   SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrtempmm10   SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrtempmm11   SET SCHEMA cipsrv_nhdplus_h;
ALTER TABLE IF EXISTS cipsrv_upload.nhdplusincrtempmm12   SET SCHEMA cipsrv_nhdplus_h;
