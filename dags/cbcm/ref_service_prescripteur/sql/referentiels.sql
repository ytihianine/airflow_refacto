CREATE SCHEMA donnee_comptable;

CREATE TABLE donnee_comptable."ref_service_prescripteur_pilotage" (
  "id" integer PRIMARY KEY,
  "service_prescripteur" text
);

CREATE TABLE donnee_comptable."ref_service_depense" (
  "id" integer PRIMARY KEY,
  "service_depense" text
);

CREATE TABLE donnee_comptable."ref_prog" (
  "id" integer PRIMARY KEY,
  "prog" text
);

CREATE TABLE donnee_comptable."ref_bop" (
  "id" integer PRIMARY KEY,
  "prog" int,
  "bop" text,
  FOREIGN KEY ("prog") REFERENCES "ref_prog" ("id")
);

CREATE TABLE donnee_comptable."ref_cc" (
  "id" integer PRIMARY KEY,
  "prog" int,
  "bop" int,
  "uo" int,
  "cc" text,
  FOREIGN KEY ("prog") REFERENCES "ref_prog" ("id"),
  FOREIGN KEY ("bop") REFERENCES "ref_bop" ("id"),
  FOREIGN KEY ("uo") REFERENCES "ref_uo" ("id")
);

CREATE TABLE donnee_comptable."ref_uo" (
  "id" integer PRIMARY KEY,
  "prog" int,
  "bop" int,
  "uo" text,
  FOREIGN KEY ("prog") REFERENCES "ref_prog" ("id"),
  FOREIGN KEY ("bop") REFERENCES "ref_bop" ("id")
);

CREATE TABLE donnee_comptable."service_prescripteur" (
  "id" integer PRIMARY KEY,
  "centre_financer" text,
  "centre_de_cout" text,
  "couple_cf_cc" text,
  "service_prescripteur_pilotage_" int,
  "service_depense" int,
  "observation" text,
  "service_prescripteur_choisi_selon_cf_cc" int,
  "designation_prog" int,
  "designation_bop" int,
  "designation_uo" int,
  "designation_cc" int,
  FOREIGN KEY ("service_prescripteur_pilotage_") REFERENCES "ref_service_prescripteur_pilotage" ("id"),
  FOREIGN KEY ("service_depense") REFERENCES "ref_service_depense" ("id"),
  FOREIGN KEY ("service_prescripteur_choisi_selon_cf_cc") REFERENCES "ref_service_prescripteur_choisi" ("id"),
  FOREIGN KEY ("designation_prog") REFERENCES "ref_prog" ("id"),
  FOREIGN KEY ("designation_bop") REFERENCES "ref_bop" ("id"),
  FOREIGN KEY ("designation_uo") REFERENCES "ref_uo" ("id"),
  FOREIGN KEY ("designation_cc") REFERENCES "ref_cc" ("id")
);

CREATE TABLE donnee_comptable."ref_service_prescripteur_choisi" (
  "id" integer PRIMARY KEY,
  "service_prescripteur" text
);
