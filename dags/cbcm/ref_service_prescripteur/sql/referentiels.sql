DROP SCHEMA donnee_comptable CASCADE;
CREATE SCHEMA donnee_comptable;

CREATE TABLE donnee_comptable."ref_service_prescripteur_pilotage" (
  "id" INTEGER PRIMARY KEY,
  "service_prescripteur" text
);

CREATE TABLE donnee_comptable."ref_service_depense" (
  "id" INTEGER PRIMARY KEY,
  "service_depense" text
);

CREATE TABLE donnee_comptable."ref_prog" (
  "id" INTEGER PRIMARY KEY,
  "prog" text
);

CREATE TABLE donnee_comptable."ref_bop" (
  "id" INTEGER PRIMARY KEY,
  "prog" int,
  "bop" text,
  FOREIGN KEY ("prog") REFERENCES donnee_comptable."ref_prog" ("id")
);

CREATE TABLE donnee_comptable."ref_uo" (
  "id" INTEGER PRIMARY KEY,
  "prog" int,
  "bop" int,
  "uo" text,
  FOREIGN KEY ("prog") REFERENCES donnee_comptable."ref_prog" ("id"),
  FOREIGN KEY ("bop") REFERENCES donnee_comptable."ref_bop" ("id")
);

CREATE TABLE donnee_comptable."ref_cc" (
  "id" INTEGER PRIMARY KEY,
  "prog" int,
  "bop" int,
  "uo" int,
  "cc" text,
  FOREIGN KEY ("prog") REFERENCES donnee_comptable."ref_prog" ("id"),
  FOREIGN KEY ("bop") REFERENCES donnee_comptable."ref_bop" ("id"),
  FOREIGN KEY ("uo") REFERENCES donnee_comptable."ref_uo" ("id")
);

CREATE TABLE donnee_comptable."ref_service_prescripteur_choisi" (
  "id" INTEGER PRIMARY KEY,
  "service_prescripteur" text
);

CREATE TABLE donnee_comptable."service_prescripteur" (
  "id" INTEGER PRIMARY KEY,
  "centre_financier" text,
  "centre_cout" text,
  "couple_cf_cc" text,
  "service_prescripteur_pilotage_" int,
  "service_depense" int,
  "observation" text,
  "service_prescripteur_choisi_selon_cf_cc" int,
  "designation_prog" int,
  "designation_bop" int,
  "designation_uo" int,
  "designation_cc" int,
  "date_creation" TIMESTAMP,
  "date_derniere_maj" TIMESTAMP,
  "doublon" int,
  FOREIGN KEY ("service_prescripteur_pilotage_") REFERENCES donnee_comptable."ref_service_prescripteur_pilotage" ("id"),
  FOREIGN KEY ("service_depense") REFERENCES donnee_comptable."ref_service_depense" ("id"),
  FOREIGN KEY ("service_prescripteur_choisi_selon_cf_cc") REFERENCES donnee_comptable."ref_service_prescripteur_choisi" ("id"),
  FOREIGN KEY ("designation_prog") REFERENCES donnee_comptable."ref_prog" ("id"),
  FOREIGN KEY ("designation_bop") REFERENCES donnee_comptable."ref_bop" ("id"),
  FOREIGN KEY ("designation_uo") REFERENCES donnee_comptable."ref_uo" ("id"),
  FOREIGN KEY ("designation_cc") REFERENCES donnee_comptable."ref_cc" ("id")
);
