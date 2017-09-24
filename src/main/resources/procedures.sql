CREATE FUNCTION films_by_actors(IN aid integer)
	RETURNS TABLE (
	 film_title VARCHAR,
	 film_description TEXT,
	 actor_name TEXT
	)
AS $$
BEGIN
 -- get the films by actor_id
	RETURN QUERY SELECT
		f.title, f.description, a.first_name || '  ' || a.last_name as Actor
	FROM
		film f
	LEFT JOIN film_actor fa
	ON
		f.film_id = fa.film_id
	LEFT JOIN actor a
	ON
		a.actor_id = fa.actor_id
	WHERE
		a.actor_id = aid;


END; $$
LANGUAGE plpgsql ;