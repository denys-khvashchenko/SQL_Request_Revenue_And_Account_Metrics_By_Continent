WITH
  session_params_cte AS (
    -- Обчислення метрик по сесіях та доходах за континентами
    SELECT
      session_params.continent AS continent,
      SUM(product.price) OVER(PARTITION BY session_params.continent) AS revenue,
      SUM(CASE WHEN session_params.device = 'mobile' THEN product.price END) OVER(PARTITION BY session_params.continent) AS revenue_form_mobile,
      SUM(CASE WHEN session_params.device = 'desktop' THEN product.price END) OVER(PARTITION BY session_params.continent) AS revenue_form_desktop,
      COUNT(session_params.ga_session_id) OVER(PARTITION BY session_params.continent) AS session_count,
      SUM(product.price) OVER() AS total_revenue
    FROM
      `DA.session_params` AS session_params
    JOIN
      `DA.order` AS orders ON session_params.ga_session_id = orders.ga_session_id
    JOIN
      `DA.product` AS product ON orders.item_id = product.item_id
  ),
  percent_revenue_from_total_cte AS (
    -- Обчислення відсотка доходу кожного континенту від загального доходу
    SELECT
      continent,
      ((revenue / total_revenue) * 100) AS percent_revenue_from_total
    FROM
      session_params_cte
  ),
  account_cte AS (
    -- Обчислення кількості облікових записів та верифікованих облікових записів за континентами
    SELECT
      session_params.continent AS continent,
      COUNT(account.id) AS account_count,
      COUNT(CASE WHEN account.is_verified = 1 THEN account_id END) AS verified_account
    FROM
      `DA.account` AS account
    JOIN
      `DA.account_session` AS account_session ON account.id = account_session.account_id
    JOIN
      `DA.session_params` AS session_params ON account_session.ga_session_id = session_params.ga_session_id
    GROUP BY
      continent
  )
-- Фінальний вибір для об'єднання всіх метрик
SELECT DISTINCT
  session_params_cte.continent,
  session_params_cte.revenue,
  session_params_cte.revenue_form_mobile,
  session_params_cte.revenue_form_desktop,
  percent_revenue_from_total_cte.percent_revenue_from_total,
  account_cte.account_count,
  account_cte.verified_account,
  session_params_cte.session_count
FROM
  session_params_cte
JOIN
  percent_revenue_from_total_cte ON session_params_cte.continent = percent_revenue_from_total_cte.continent
JOIN
  account_cte ON session_params_cte.continent = account_cte.continent