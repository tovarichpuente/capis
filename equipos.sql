BEGIN TRANSACTION READ ONLY;

with equipos as 
(SELECT id, 

 case when id in (45230,42827,44924,45227,45602,45957,41460,43453,43230,43386,43752,43144,42997,43723,43389,43044,43644,43703,43427,43137,42964,43358,43075,43045,43030,43449,41111,43702,44728,34487,41581,45160) then 'Team Bravo'
when id in (38042,30344,37105,30364,41279,41568,40225,40855,32561,34478,41457,42280,35954,33086,42116,42247,44812,43694,43645,40279) then 'Team Fox'
when id in (43895,33578,44133,43917,32126,42593,36790,38471,32903,41933,33117,38725,30363,33346,30566,34430,38805,35572,39081,38466,31304,38952,30281,33349,38380,38709,33081,43689,37584,36509,32147,30077,37910,33612,37451,45532,36101,44748) then 'Team Delta'
when id in (41496,31667,33060,40693,40038,39773,37042,40695,42254,37537,40216,43822,44459,42431,35331,44065,46031,46268,46404,45885,46588,46633,46724,46746,46763) then 'Team Echo'
when id in (33064,37922,35399,34606,39076,39913,36741,41650,40954,30036,41555,37359,33852,35468,30058,30034,40321,44465,44391,44291,44340,42436,33232) then 'Team Charlie'
when id in (40328,45290,45965,35756,32303,31337,44394,32549,40630,45213,39966,38167,34989,45559,41275,46766,45883,38624,38469,33476,46246,42619,36741,33001,38194,42767,44463,37786,33349,36966,40303,31493,31933,36559) then 'Team Bull'
else null end as equipo

FROM storekeepers),

conectividad as
(SELECT  equipo, sum(extract(epoch from ends_at - starts_at)/3600) horas_conexion_totales
 FROM storekeeper_sessions ss
 LEFT JOIN equipos e ON ss.storekeeper_id = e.id
 WHERE date(starts_at) between '[From]' AND '[To]'
GROUP BY 1),

ordenes as
(SELECT coalesce(deliveryboy_id,storekeeper_id) storekeeper_id, lp_zone(lat::text,lng::text) zona, count(o.id) ordenes
FROM orders o
LEFT JOIN user_addresses ua ON o.address_id = ua.id
WHERE date(o.created_at) between '[From]' AND '[To]'
AND o.state in ('on_appeal','pending_review','storekeeper_payment_in_analysis','paid_to_sk','storekeeper_payment_canceled','paid_verification_fail','finished','split_requested')
GROUP BY 1,2),

rating as
(SELECT coalesce(deliveryboy_id,storekeeper_id) storekeeper_id, lp_zone(lat::text,lng::text) zona, avg(rate) rate
FROM orders o
LEFT JOIN surveys s ON o.id = s.order_id 
LEFT JOIN user_addresses ua ON o.address_id = ua.id
WHERE date(o.created_at) between '[From]' AND '[To]'
AND o.state in ('on_appeal','pending_review','storekeeper_payment_in_analysis','paid_to_sk','storekeeper_payment_canceled','paid_verification_fail','finished','split_requested')
 AND qualifier_type = 'ApplicationUser'
GROUP BY 1,2),

taken as
(SELECT order_id, min(created_at) AS taken
 FROM order_modifications
 WHERE type in ('taken_visible_order')
 GROUP BY 1),

replace_storekeeper as
(SELECT order_id, max(created_at) AS replace_storekeeper
 FROM order_modifications
 WHERE type in ('replace_storekeeper')
 GROUP BY 1),


 arrive as
(SELECT order_id, created_at AS arrive
 FROM order_modifications
 WHERE type = 'arrive'),

tiempos as
(SELECT coalesce(deliveryboy_id,storekeeper_id) storekeeper_id, lp_zone(lat::text,lng::text) zona, avg(extract(epoch from coalesce(arrive,closed_at) - coalesce(taken,replace_storekeeper,o.taked_at,o.created_at))/60) tiempo_promedio
FROM orders o
LEFT JOIN taken t ON o.id = t.order_id 
LEFT JOIN arrive a ON o.id = a.order_id 
LEFT JOIN replace_storekeeper rs ON o.id = rs.order_id 
LEFT JOIN user_addresses ua ON o.address_id = ua.id
WHERE date(o.created_at) between '[From]' AND '[To]'
AND o.state in ('on_appeal','pending_review','storekeeper_payment_in_analysis','paid_to_sk','storekeeper_payment_canceled','paid_verification_fail','finished','split_requested')
GROUP BY 1,2),

info as 
(SELECT equipo, o.zona, sum(ordenes) pedidos_totales,  avg(rate) rating_stars, avg(tiempo_promedio) tiempo_promedio
FROM storekeepers st
LEFT JOIN ordenes o ON st.id = o.storekeeper_id
LEFT JOIN rating r ON st.id = r.storekeeper_id AND o.zona = r.zona
LEFT JOIN tiempos t ON st.id = t.storekeeper_id AND o.zona = t.zona
 LEFT JOIN equipos e ON st.id = e.id
WHERE o.zona ilike '%mty%'
AND o.zona not like '%N/A%'
GROUP BY 1,2),

maximos as
(SELECT zona, max(pedidos_totales) max_pedidos, max(rating_stars) max_rating, min(tiempo_promedio) max_tiempo
FROM info
WHERE zona ilike '%mty%'
AND zona not like '%N/A%'
AND equipo is not null
GROUP BY 1),

info_pedidos as
(SELECT equipo, i.zona, 

pedidos_totales, case when pedidos_totales = max_pedidos then 1 else 0 end as puntos_pt,

rating_stars, case when rating_stars = max_rating then 1 else 0 end as puntos_rs,

tiempo_promedio, case when tiempo_promedio = max_tiempo then 1 else 0 end as puntos_tp

FROM info i
LEFT JOIN maximos m ON i.zona = m.zona
WHERE equipo is not null
AND i.zona is not null),

total_pedidos as 
(SELECT equipo, sum(pedidos_totales) pedidos_totales, sum(puntos_pt) puntos_pt, avg(rating_stars) rating_stars, sum(puntos_rs) puntos_rs, avg(tiempo_promedio) tiempo_promedio, sum(puntos_tp) puntos_tp
FROM info_pedidos 
GROUP BY 1)

SELECT tp.equipo, puntos_pt + puntos_rs + puntos_tp + (case when horas_conexion_totales = (select max(horas_conexion_totales) from conectividad where equipo is not null) then 1 else 0 end) puntos_totales, pedidos_totales, puntos_pt, rating_stars, puntos_rs, tiempo_promedio, puntos_tp, horas_conexion_totales, case when horas_conexion_totales = (select max(horas_conexion_totales) from conectividad where equipo is not null) then 1 else 0 end as puntos_con
FROM total_pedidos tp 
LEFT JOIN conectividad c ON tp.equipo = c.equipo
ORDER BY 2 desc