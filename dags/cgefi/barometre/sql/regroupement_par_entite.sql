-----------------------------------------------------
-- Toutes les métrics propres au dataset Baromètre --
-----------------------------------------------------

-- Tous les rapports attendus // nb_rapports_attendus
SUM(CASE
  WHEN type_rapport IN ('Principal', 'Rattaché') THEN 1
  ELSE 0
END)

-- Tous les rapports principaux attendus // nb_rapports_principaux_attendus
SUM(CASE
  WHEN type_rapport IN ('Principal') THEN 1
  ELSE 0
END)

-- Tous les rapports rattachés attendus // nb_rapports_rattaches_attendus
SUM(CASE
  WHEN type_rapport IN ('Rattaché') THEN 1
  ELSE 0
END)

-- Tous les rapports publiés // nb_rapports_publies
SUM(CASE
  WHEN type_rapport IN ('Principal', 'Rattaché')
  AND date_du_retour is not NULL THEN 1
  ELSE 0
END)

-- Tous les rapports principaux publiés // nb_rapports_principaux_publies
SUM(CASE
  WHEN type_rapport IN ('Principal')
  AND date_du_retour is not NULL THEN 1
  ELSE 0
END)


-- Tous les rapports rattachés publiés // nb_rapports_rattaches_publies
SUM(CASE
  WHEN type_rapport IN ('Rattaché')
  AND date_du_retour is not NULL THEN 1
  ELSE 0
END)

-- Taux de retour de rapports // taux_retour_rapports
CAST(SUM(CASE
  WHEN type_rapport IN ('Principal', 'Rattaché')
  AND date_du_retour is not NULL THEN 1
  ELSE 0
END) AS DECIMAL(10, 2))/ NULLIF(SUM(CASE
  WHEN type_rapport IN ('Principal', 'Rattaché') THEN 1
  ELSE 0
END), 0)

-- Taux de retour de rapports principaux // taux_retour_rapports_principaux
CAST(SUM(CASE
  WHEN type_rapport IN ('Principal')
  AND date_du_retour is not NULL THEN 1
  ELSE 0
END) AS DECIMAL(10, 2))/ NULLIF(SUM(CASE
  WHEN type_rapport IN ('Principal') THEN 1
  ELSE 0
END), 0)

-- Taux de retour de rapports rattachés // taux_retour_rapports_rattaches
CAST(SUM(CASE
  WHEN type_rapport IN ('Rattaché')
  AND date_du_retour is not NULL THEN 1
  ELSE 0
END) AS DECIMAL(10, 2))/ NULLIF(SUM(CASE
  WHEN type_rapport IN ('Rattaché') THEN 1
  ELSE 0
END), 0)

-- Taux de retour des rapports par entité // taux_retour_rapports_par_entite
COALESCE(
CAST(SUM(rapport_realise) AS DECIMAL(10, 2))
/
AVG(rapports_attendus_by_entite)
, 0)

-- Taux de retour des rapports // taux_retour_rapports
COALESCE(
CAST(SUM(rapport_realise) AS DECIMAL(10, 2))
/
AVG(rapports_attendus_total)
, 0)
