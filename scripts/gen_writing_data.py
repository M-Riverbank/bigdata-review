import json
import os

questions = []

question_data = [
    {
        'id': 'ssql-w-001',
        'difficulty': 'medium',
        'stem': '有一个订单表 orders(order_id, user_id, product_id, amount, order_date)，请用 Spark SQL 查询每个用户下单金额最高的前 3 笔订单。',
        'tables': [
            {
                'name': 'orders',
                'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| order_id | STRING | 订单ID |\n| user_id | INT | 用户ID |\n| product_id | STRING | 商品ID |\n| amount | DOUBLE | 订单金额 |\n| order_date | STRING | 下单日期 |'
            }
        ],
        'referencePoints': [
            '使用 ROW_NUMBER() 或 RANK() 窗口函数',
            'PARTITION BY user_id 按用户分区',
            'ORDER BY amount DESC 按金额降序排序',
            '筛选 rn <= 3 取前三'
        ],
        'sampleAnswer': 'WITH ranked AS (\n  SELECT *,\n    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC) AS rn\n  FROM orders\n)\nSELECT user_id, order_id, amount, order_date\nFROM ranked WHERE rn <= 3'
    },
    {
        'id': 'ssql-w-002',
        'difficulty': 'hard',
        'stem': '有一个用户登录表 user_login(user_id INT, login_date STRING)，请找出连续登录 3 天及以上的用户，并输出每个连续登录段的起始日期和结束日期。',
        'tables': [
            {
                'name': 'user_login',
                'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| user_id | INT | 用户ID |\n| login_date | STRING | 登录日期 (格式: yyyy-MM-dd) |'
            }
        ],
        'referencePoints': [
            '先 DISTINCT 去重避免同一天多次登录',
            '使用 ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) 生成行号',
            '核心技巧：login_date - rn 得到分组标识',
            '按 user_id 和分组标识 GROUP BY 并统计天数',
            'HAVING COUNT(*) >= 3 筛选连续3天以上'
        ],
        'sampleAnswer': 'WITH daily AS (\n  SELECT DISTINCT user_id, login_date\n  FROM user_login\n),\ntmp AS (\n  SELECT user_id, login_date,\n    DATE_SUB(login_date, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date)) AS grp\n  FROM daily\n)\nSELECT user_id, MIN(login_date) AS start_date, MAX(login_date) AS end_date, COUNT(*) AS days\nFROM tmp\nGROUP BY user_id, grp\nHAVING COUNT(*) >= 3\nORDER BY user_id, start_date'
    },
    {
        'id': 'ssql-w-003',
        'difficulty': 'medium',
        'stem': '有一个学生成绩表 scores(student_id INT, subject STRING, score INT)，科目包括 chinese(语文)、math(数学)、english(英语)。请用 Spark SQL 行转列，输出每个学生三科成绩的宽表格式：student_id, chinese, math, english。',
        'tables': [
            {
                'name': 'scores',
                'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| student_id | INT | 学生ID |\n| subject | STRING | 科目 |\n| score | INT | 成绩 |',
                'data': '| student_id | subject | score |\n|------------|---------|-------|\n| 1 | chinese | 85 |\n| 1 | math | 92 |\n| 1 | english | 88 |\n| 2 | chinese | 78 |\n| 2 | math | 85 |\n| 2 | english | 90 |'
            }
        ],
        'referencePoints': [
            '使用 GROUP BY student_id 聚合',
            '使用 CASE WHEN + MAX 或 SUM 进行行转列',
            '为每个科目创建一个列',
            '注意：GROUP BY 后需要用聚合函数包裹 CASE WHEN'
        ],
        'sampleAnswer': 'SELECT student_id,\n  MAX(CASE WHEN subject = "chinese" THEN score END) AS chinese,\n  MAX(CASE WHEN subject = "math" THEN score END) AS math,\n  MAX(CASE WHEN subject = "english" THEN score END) AS english\nFROM scores\nGROUP BY student_id'
    },
    {
        'id': 'ssql-w-004',
        'difficulty': 'hard',
        'stem': '有一个用户登录表 login_records(user_id INT, login_time STRING)，login_time 是精确到秒的时间戳（格式：yyyy-MM-dd HH:mm:ss）。请计算每日的活跃用户数(DAU)，以及每个用户上次登录距现在的平均间隔天数。',
        'tables': [
            {
                'name': 'login_records',
                'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| user_id | INT | 用户ID |\n| login_time | STRING | 登录时间 (yyyy-MM-dd HH:mm:ss) |'
            }
        ],
        'referencePoints': [
            '使用 TO_DATE 或 DATE 函数提取日期',
            'DAU 计算：按日期 GROUP BY 并 COUNT DISTINCT user_id',
            '使用 LAG() 窗口函数获取上次登录时间',
            '计算登录间隔天数：DATEDIFF(当前登录, 上次登录)',
            '计算所有用户的平均间隔'
        ],
        'sampleAnswer': 'WITH daily_users AS (\n  SELECT DISTINCT\n    TO_DATE(login_time) AS login_date,\n    user_id\n  FROM login_records\n),\nweekly AS (\n  SELECT\n    login_date,\n    COUNT(DISTINCT user_id) AS dau\n  FROM daily_users\n  GROUP BY login_date\n),\nuser_intervals AS (\n  SELECT user_id, login_date,\n    LAG(login_date) OVER (PARTITION BY user_id ORDER BY login_date) AS prev_login\n  FROM daily_users\n)\nSELECT\n  w.login_date,\n  w.dau,\n  ROUND(AVG(DATEDIFF(ui.login_date, ui.prev_login)), 2) AS avg_interval_days\nFROM weekly w\nJOIN user_intervals ui ON w.login_date = ui.login_date\nWHERE ui.prev_login IS NOT NULL\nGROUP BY w.login_date, w.dau\nORDER BY w.login_date'
    },
    {
        'id': 'ssql-w-005',
        'difficulty': 'easy',
        'stem': '有一个员工表 employees(id INT, name STRING, department_id INT, salary DOUBLE)，请查询每个部门的平均工资，输出 department_id 和 avg_salary，并按平均工资降序排列。',
        'tables': [{'name': 'employees', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| id | INT | 员工ID |\n| name | STRING | 员工姓名 |\n| department_id | INT | 所属部门ID |\n| salary | DOUBLE | 薪资 |'}],
        'referencePoints': [
            '使用 GROUP BY department_id',
            '使用 AVG(salary) 计算平均工资',
            '使用 ORDER BY avg_salary DESC 降序排列',
            '可以使用 ROUND 函数保留小数'
        ],
        'sampleAnswer': 'SELECT department_id,\n  ROUND(AVG(salary), 2) AS avg_salary\nFROM employees\nGROUP BY department_id\nORDER BY avg_salary DESC'
    },
    {
        'id': 'ssql-w-006',
        'difficulty': 'medium',
        'stem': '有两个表：employees(id INT, name STRING, department_id INT, salary DOUBLE) 和 departments(id INT, name STRING)。请查询每个部门工资最高的员工姓名和工资，输出 department_name, employee_name, salary。',
        'tables': [
            {'name': 'employees', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| id | INT | 员工ID |\n| name | STRING | 员工姓名 |\n| department_id | INT | 所属部门ID |\n| salary | DOUBLE | 薪资 |'},
            {'name': 'departments', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| id | INT | 部门ID |\n| name | STRING | 部门名称 |'}
        ],
        'referencePoints': [
            '使用 ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC)',
            '筛选 rn=1 取每个部门工资最高的人',
            'JOIN departments 获取部门名称',
            '正确使用 JOIN 条件'
        ],
        'sampleAnswer': 'WITH ranked AS (\n  SELECT e.*, d.name AS department_name,\n    ROW_NUMBER() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS rn\n  FROM employees e\n  JOIN departments d ON e.department_id = d.id\n)\nSELECT department_name, name AS employee_name, salary\nFROM ranked WHERE rn = 1'
    },
    {
        'id': 'ssql-w-007',
        'difficulty': 'hard',
        'stem': '有一个交易表 transactions(user_id INT, transaction_amount DOUBLE, transaction_time STRING)，请计算每个用户消费金额的累计总和（按交易时间正序），并输出 user_id, transaction_time, amount, cumulative_amount。',
        'tables': [{'name': 'transactions', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| user_id | INT | 用户ID |\n| transaction_amount | DOUBLE | 交易金额 |\n| transaction_time | STRING | 交易时间 (yyyy-MM-dd HH:mm:ss) |'}],
        'referencePoints': [
            '使用 SUM() OVER 窗口函数计算累计和',
            'PARTITION BY user_id 按用户分区',
            'ORDER BY transaction_time 按时间排序',
            '注意默认窗口范围是 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW'
        ],
        'sampleAnswer': 'SELECT user_id, transaction_time, transaction_amount,\n  SUM(transaction_amount) OVER (\n    PARTITION BY user_id\n    ORDER BY transaction_time\n  ) AS cumulative_amount\nFROM transactions\nORDER BY user_id, transaction_time'
    },
    {
        'id': 'ssql-w-008',
        'difficulty': 'medium',
        'stem': '有一个产品表 products(product_id STRING, product_name STRING, category STRING, price DOUBLE)，请查询每个品类下价格高于该品类平均价格的产品列表，输出 product_id, product_name, category, price。',
        'tables': [{'name': 'products', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| product_id | STRING | 产品ID |\n| product_name | STRING | 产品名称 |\n| category | STRING | 产品品类 |\n| price | DOUBLE | 价格 |'}],
        'referencePoints': [
            '使用 AVG() OVER (PARTITION BY category) 计算品类均价',
            '在外层筛选 price > avg_price',
            '也可以使用 JOIN 子查询的方式',
            '窗口函数的方式更简洁高效'
        ],
        'sampleAnswer': 'WITH tmp AS (\n  SELECT *,\n    AVG(price) OVER (PARTITION BY category) AS category_avg_price\n  FROM products\n)\nSELECT product_id, product_name, category, price\nFROM tmp\nWHERE price > category_avg_price\nORDER BY category, price DESC'
    },
    {
        'id': 'ssql-w-009',
        'difficulty': 'easy',
        'stem': '有一个表 users(id INT, name STRING, age INT, city STRING)，请查询来自北京(BJ)的用户中年龄大于 25 岁的用户，按年龄降序输出 id, name, age。',
        'tables': [{'name': 'users', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| id | INT | 用户ID |\n| name | STRING | 姓名 |\n| age | INT | 年龄 |\n| city | STRING | 所在城市 |'}],
        'referencePoints': [
            'WHERE city = "BJ" 做条件过滤',
            'WHERE age > 25 筛选年龄条件',
            'ORDER BY age DESC 降序排列'
        ],
        'sampleAnswer': 'SELECT id, name, age\nFROM users\nWHERE city = "BJ" AND age > 25\nORDER BY age DESC'
    },
    {
        'id': 'ssql-w-010',
        'difficulty': 'medium',
        'stem': '有两个表：orders(order_id INT, user_id INT, amount DOUBLE, order_date STRING) 和 users(user_id INT, name STRING, city STRING)。请查询来自上海(SH)的用户在 2024 年的总消费金额，输出 user_id, name, total_amount。',
        'tables': [
            {'name': 'orders', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| order_id | INT | 订单ID |\n| user_id | INT | 用户ID |\n| amount | DOUBLE | 订单金额 |\n| order_date | STRING | 下单日期 |'},
            {'name': 'users', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| user_id | INT | 用户ID |\n| name | STRING | 姓名 |\n| city | STRING | 所在城市 |'}
        ],
        'referencePoints': [
            'INNER JOIN users 和 orders',
            'WHERE 筛选城市为上海和 2024 年订单',
            'GROUP BY 按用户聚合',
            'SUM(amount) 计算总消费',
            '使用 YEAR(order_date) 或字符串匹配过滤年份'
        ],
        'sampleAnswer': 'SELECT u.user_id, u.name,\n  SUM(o.amount) AS total_amount\nFROM users u\nJOIN orders o ON u.user_id = o.user_id\nWHERE u.city = "SH"\n  AND o.order_date LIKE "2024%"\nGROUP BY u.user_id, u.name\nORDER BY total_amount DESC'
    },
    {
        'id': 'ssql-w-011',
        'difficulty': 'hard',
        'stem': '有一个页面访问表 page_views(user_id INT, page_url STRING, view_time STRING)，view_time 格式为 yyyy-MM-dd HH:mm:ss。请找出每个用户访问路径中的"上一页" URL，即按时间排序后前一行的 page_url。',
        'tables': [{'name': 'page_views', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| user_id | INT | 用户ID |\n| page_url | STRING | 访问的页面URL |\n| view_time | STRING | 访问时间 (yyyy-MM-dd HH:mm:ss) |'}],
        'referencePoints': [
            '使用 LAG() 窗口函数获取前一行',
            'PARTITION BY user_id 按用户分区',
            'ORDER BY view_time 按访问时间排序',
            'LAG(page_url, 1) 取上一次访问的 URL'
        ],
        'sampleAnswer': 'SELECT user_id, page_url, view_time,\n  LAG(page_url, 1) OVER (\n    PARTITION BY user_id\n    ORDER BY view_time\n  ) AS previous_page\nFROM page_views\nORDER BY user_id, view_time'
    },
    {
        'id': 'ssql-w-012',
        'difficulty': 'easy',
        'stem': '有一个销售表 sales(id INT, product STRING, quantity INT, sale_date STRING)。请统计每种产品的总销售量，输出 product 和 total_quantity，并按总销量降序排列。',
        'tables': [{'name': 'sales', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| id | INT | 销售记录ID |\n| product | STRING | 产品名称 |\n| quantity | INT | 销售数量 |\n| sale_date | STRING | 销售日期 |'}],
        'referencePoints': [
            'GROUP BY product 按产品分组',
            'SUM(quantity) 计算总销量',
            'ORDER BY total_quantity DESC 降序',
            '可使用 COALESCE 处理 NULL'
        ],
        'sampleAnswer': 'SELECT product,\n  SUM(quantity) AS total_quantity\nFROM sales\nGROUP BY product\nORDER BY total_quantity DESC'
    },
    {
        'id': 'ssql-w-013',
        'difficulty': 'medium',
        'stem': '有一个表 employees(id INT, name STRING, manager_id INT, salary DOUBLE, department STRING)。manager_id 指向同一表中 id 字段（即上级的 id）。请查询每个员工及其上级的姓名，输出 employee_name, manager_name，如果没有上级则 manager_name 显示为"BOSS"。',
        'tables': [{'name': 'employees', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| id | INT | 员工ID |\n| name | STRING | 姓名 |\n| manager_id | INT | 上级ID (可为 NULL) |\n| salary | DOUBLE | 薪资 |\n| department | STRING | 所属部门 |'}],
        'referencePoints': [
            '使用自关联 (LEFT JOIN) 连接 employees 自身',
            'LEFT JOIN 保证 manager_id 为 NULL 的记录也保留',
            '使用 COALESCE 或 CASE WHEN 处理 NULL 显示为 BOSS'
        ],
        'sampleAnswer': 'SELECT e.name AS employee_name,\n  COALESCE(m.name, "BOSS") AS manager_name\nFROM employees e\nLEFT JOIN employees m ON e.manager_id = m.id'
    },
    {
        'id': 'ssql-w-014',
        'difficulty': 'medium',
        'stem': '有一个课程选课表 enrollments(student_id INT, course_id INT, score INT)。请找出所有选修了课程 ID 为 101 的学生中，成绩高于该课程平均分的学生列表，输出 student_id, score。',
        'tables': [{'name': 'enrollments', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| student_id | INT | 学生ID |\n| course_id | INT | 课程ID |\n| score | INT | 成绩 |'}],
        'referencePoints': [
            '先过滤 course_id = 101 的学生',
            '计算该课程的平均分',
            '对比每个学生的成绩是否高于平均分',
            '可以使用子查询或窗口函数实现'
        ],
        'sampleAnswer': 'WITH course_avg AS (\n  SELECT AVG(score) AS avg_score\n  FROM enrollments\n  WHERE course_id = 101\n)\nSELECT student_id, score\nFROM enrollments, course_avg\nWHERE course_id = 101\n  AND score > course_avg.avg_score'
    },
    {
        'id': 'ssql-w-015',
        'difficulty': 'medium',
        'stem': '有一个订单表 orders(order_id INT, user_id INT, amount DOUBLE, order_date STRING)。请计算每个月的总销售额和累计销售额（从年初到当月的累计），输出 year_month, monthly_revenue, cumulative_revenue。',
        'tables': [{'name': 'orders', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| order_id | INT | 订单ID |\n| user_id | INT | 用户ID |\n| amount | DOUBLE | 订单金额 |\n| order_date | STRING | 下单日期 (yyyy-MM-dd) |'}],
        'referencePoints': [
            '使用 SUBSTR 或 DATE_FORMAT 提取年月',
            '先按月 GROUP BY 汇总月销售额',
            '使用 SUM() OVER (ORDER BY 年月) 计算累计',
            '注意年界限：跨年时累计重置'
        ],
        'sampleAnswer': 'WITH monthly AS (\n  SELECT\n    SUBSTR(order_date, 1, 7) AS year_month,\n    SUM(amount) AS monthly_revenue\n  FROM orders\n  GROUP BY SUBSTR(order_date, 1, 7)\n)\nSELECT year_month, monthly_revenue,\n  SUM(monthly_revenue) OVER (ORDER BY year_month) AS cumulative_revenue\nFROM monthly\nORDER BY year_month'
    },
    {
        'id': 'ssql-w-016',
        'difficulty': 'hard',
        'stem': '有一个表 user_actions(user_id INT, action STRING, action_time STRING)，action 取值为 "login" 或 "logout"，action_time 为时间戳。请找出每天最后登录但未退出的用户（即当天只有 login 没有 logout 的用户）。',
        'tables': [{'name': 'user_actions', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| user_id | INT | 用户ID |\n| action | STRING | 动作类型 ("login" / "logout") |\n| action_time | STRING | 操作时间 (yyyy-MM-dd HH:mm:ss) |'}],
        'referencePoints': [
            '提取日期后按用户和日期分组',
            '使用 MAX(CASE WHEN action = "logout" ...) 判断是否有 logout',
            '筛选有 login 但无 logout 的组合',
            '注意处理同一天多次 login/logout 的情况'
        ],
        'sampleAnswer': 'WITH daily AS (\n  SELECT DISTINCT\n    TO_DATE(action_time) AS action_date,\n    user_id, action\n  FROM user_actions\n)\nSELECT action_date, user_id\nFROM daily\nGROUP BY action_date, user_id\nHAVING\n  SUM(CASE WHEN action = "login" THEN 1 ELSE 0 END) > 0\n  AND SUM(CASE WHEN action = "logout" THEN 1 ELSE 0 END) = 0\nORDER BY action_date, user_id'
    },
    {
        'id': 'ssql-w-017',
        'difficulty': 'hard',
        'stem': '有一个流量表 traffic(source STRING, user_id INT, page_views INT, visit_date STRING)，记录了不同来源各用户的页面浏览量。请按来源分组，计算每天的累计 PageView（即截至当天每个来源的累计 PV）。输出 source, visit_date, daily_pv, cumulative_pv。',
        'tables': [{'name': 'traffic', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| source | STRING | 流量来源 |\n| user_id | INT | 访客ID |\n| page_views | INT | 页面浏览量 |\n| visit_date | STRING | 访问日期 |'}],
        'referencePoints': [
            '先按 source 和 visit_date 聚合计算每天 PV',
            '使用 SUM() OVER (PARTITION BY source ORDER BY visit_date) 计算累计 PV',
            '注意窗口函数的执行顺序',
            'ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW 是默认范围'
        ],
        'sampleAnswer': 'WITH daily_pv AS (\n  SELECT source, visit_date,\n    SUM(page_views) AS pv\n  FROM traffic\n  GROUP BY source, visit_date\n)\nSELECT source, visit_date, pv,\n  SUM(pv) OVER (\n    PARTITION BY source\n    ORDER BY visit_date\n    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n  ) AS cumulative_pv\nFROM daily_pv\nORDER BY source, visit_date'
    },
    {
        'id': 'ssql-w-018',
        'difficulty': 'easy',
        'stem': '有一个表 students(id INT, name STRING, score INT)。请查询成绩排名第二的学生信息（假设没有并列成绩），输出 id, name, score。',
        'tables': [{'name': 'students', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| id | INT | 学生ID |\n| name | STRING | 学生姓名 |\n| score | INT | 成绩 |'}],
        'referencePoints': [
            '使用 ORDER BY score DESC LIMIT 1 OFFSET 1',
            '也可以使用 ROW_NUMBER() 窗口函数',
            '第二种方法更通用'
        ],
        'sampleAnswer': 'SELECT id, name, score\nFROM students\nORDER BY score DESC\nLIMIT 1 OFFSET 1'
    },
    {
        'id': 'ssql-w-019',
        'difficulty': 'easy',
        'stem': '有一个表 products(product_id INT, product_name STRING, price DOUBLE, stock INT)。请查询库存大于 0 且价格在 50 到 200 之间的产品，按价格升序输出 product_id, product_name, price。',
        'tables': [{'name': 'products', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| product_id | INT | 产品ID |\n| product_name | STRING | 产品名称 |\n| price | DOUBLE | 价格 |\n| stock | INT | 库存量 |'}],
        'referencePoints': [
            'WHERE stock > 0 AND price BETWEEN 50 AND 200',
            'ORDER BY price ASC',
            '也可以使用 price >= 50 AND price <= 200'
        ],
        'sampleAnswer': 'SELECT product_id, product_name, price\nFROM products\nWHERE stock > 0\n  AND price BETWEEN 50 AND 200\nORDER BY price ASC'
    },
    {
        'id': 'ssql-w-020',
        'difficulty': 'medium',
        'stem': '有一个表 employee_attendance(emp_id INT, date STRING, status STRING)，status 取值为 "present"(出勤)、"absent"(缺勤)、"leave"(请假)。请统计每个月每个员工的出勤天数，输出 year_month, emp_id, attendance_days。',
        'tables': [{'name': 'employee_attendance', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| emp_id | INT | 员工ID |\n| date | STRING | 考勤日期 (yyyy-MM-dd) |\n| status | STRING | 状态 (present/absent/leave) |'}],
        'referencePoints': [
            '使用 SUBSTR 提取年月',
            'WHERE status = "present" 过滤出勤',
            'GROUP BY SUBSTR(date, 1, 7), emp_id',
            'COUNT(*) 统计出勤天数'
        ],
        'sampleAnswer': 'SELECT\n  SUBSTR(date, 1, 7) AS year_month,\n  emp_id,\n  COUNT(*) AS attendance_days\nFROM employee_attendance\nWHERE status = "present"\nGROUP BY SUBSTR(date, 1, 7), emp_id\nORDER BY year_month, emp_id'
    },
    {
        'id': 'ssql-w-021',
        'difficulty': 'medium',
        'stem': '有一个表 itembought(user_id INT, item_name STRING, price DOUBLE, buy_date STRING)。请查询每个用户购买次数最多的商品（即最常买的商品），输出 user_id, item_name, buy_count。',
        'tables': [{'name': 'itembought', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| user_id | INT | 用户ID |\n| item_name | STRING | 商品名称 |\n| price | DOUBLE | 商品价格 |\n| buy_date | STRING | 购买日期 |'}],
        'referencePoints': [
            '先按 user_id, item_name 聚合统计购买次数',
            '使用 ROW_NUMBER() 窗口函数按购买次数降序排列',
            'PARTITION BY user_id 每个用户独立排序',
            '筛选 rn=1 取购买次数最多的商品'
        ],
        'sampleAnswer': 'WITH purchase_count AS (\n  SELECT user_id, item_name,\n    COUNT(*) AS buy_count\n  FROM itembought\n  GROUP BY user_id, item_name\n),\nranked AS (\n  SELECT user_id, item_name, buy_count,\n    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY buy_count DESC) AS rn\n  FROM purchase_count\n)\nSELECT user_id, item_name, buy_count\nFROM ranked WHERE rn = 1'
    },
    {
        'id': 'ssql-w-022',
        'difficulty': 'hard',
        'stem': '有一个表 user_retention(user_id INT, register_date STRING, login_date STRING)。请计算每日新用户在注册后次日、3日后的留存率。输出格式：register_date, new_users, day1_retention_rate, day3_retention_rate。',
        'tables': [{'name': 'user_retention', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| user_id | INT | 用户ID |\n| register_date | STRING | 注册日期 |\n| login_date | STRING | 登录日期 |'}],
        'referencePoints': [
            '先计算每日新增用户数',
            'LEFT JOIN 自身匹配次日/3日后的登录记录',
            'DATEDIFF(login_date, register_date) 计算间隔',
            '留存率 = 留存用户数 / 新增用户数 * 100%'
        ],
        'sampleAnswer': 'WITH new_users AS (\n  SELECT register_date,\n    COUNT(DISTINCT user_id) AS new_user_count\n  FROM user_retention\n  GROUP BY register_date\n),\nretention AS (\n  SELECT\n    r.register_date,\n    COUNT(DISTINCT CASE WHEN DATEDIFF(l.login_date, r.register_date) = 1 THEN r.user_id END) AS day1_users,\n    COUNT(DISTINCT CASE WHEN DATEDIFF(l.login_date, r.register_date) = 3 THEN r.user_id END) AS day3_users\n  FROM (SELECT DISTINCT user_id, register_date FROM user_retention) r\n  LEFT JOIN (SELECT DISTINCT user_id, login_date FROM user_retention) l\n    ON r.user_id = l.user_id\n  GROUP BY r.register_date\n)\nSELECT\n  n.register_date,\n  n.new_user_count,\n  ROUND(r.day1_users * 100.0 / n.new_user_count, 2) AS day1_retention_rate,\n  ROUND(r.day3_users * 100.0 / n.new_user_count, 2) AS day3_retention_rate\nFROM new_users n\nJOIN retention r ON n.register_date = r.register_date\nORDER BY n.register_date'
    },
    {
        'id': 'ssql-w-023',
        'difficulty': 'medium',
        'stem': '有一个表 delivery_orders(order_id STRING, courier_id INT, delivery_time STRING, status STRING)，status 值为 "delivered"(已送达) 或 "cancelled"(已取消)。请统计每个配送员的完成订单数和取消订单数，以及完成率，输出 courier_id, completed, cancelled, completion_rate。',
        'tables': [{'name': 'delivery_orders', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| order_id | STRING | 订单ID |\n| courier_id | INT | 配送员ID |\n| delivery_time | STRING | 配送时间 |\n| status | STRING | 状态 (delivered/cancelled) |'}],
        'referencePoints': [
            'GROUP BY courier_id',
            'COUNT(*) 总订单数',
            'SUM(CASE WHEN status = "delivered" THEN 1 ELSE 0 END) 统计完成数',
            '完成率 = 完成数 / 总订单数 * 100%'
        ],
        'sampleAnswer': 'SELECT courier_id,\n  SUM(CASE WHEN status = "delivered" THEN 1 ELSE 0 END) AS completed,\n  SUM(CASE WHEN status = "cancelled" THEN 1 ELSE 0 END) AS cancelled,\n  ROUND(\n    SUM(CASE WHEN status = "delivered" THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2\n  ) AS completion_rate\nFROM delivery_orders\nGROUP BY courier_id\nORDER BY completion_rate DESC'
    },
    {
        'id': 'ssql-w-024',
        'difficulty': 'easy',
        'stem': '有一个表 weather(city STRING, temperature DOUBLE, date STRING)。查询每个城市最高气温和最低气温的差值（温差），输出 city, temp_range，按温差降序排列。',
        'tables': [{'name': 'weather', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| city | STRING | 城市 |\n| temperature | DOUBLE | 气温 |\n| date | STRING | 日期 |'}],
        'referencePoints': [
            'GROUP BY city',
            'MAX(temperature) - MIN(temperature) 计算温差',
            'ORDER BY temp_range DESC 降序排列'
        ],
        'sampleAnswer': 'SELECT city,\n  MAX(temperature) - MIN(temperature) AS temp_range\nFROM weather\nGROUP BY city\nORDER BY temp_range DESC'
    },
    {
        'id': 'ssql-w-025',
        'difficulty': 'hard',
        'stem': '有一个表 clickstream(session_id STRING, user_id INT, event_time STRING, event_type STRING, page_url STRING)，每个 session 包含多个事件。请找出每个 session 中用户首次访问的页面（entry page）和最后的页面（exit page），输出 session_id, entry_page, exit_page, total_events。',
        'tables': [{'name': 'clickstream', 'schema': '| 列名 | 类型 | 说明 |\n|------|------|------|\n| session_id | STRING | 会话ID |\n| user_id | INT | 用户ID |\n| event_time | STRING | 事件时间 |\n| event_type | STRING | 事件类型 |\n| page_url | STRING | 页面URL |'}],
        'referencePoints': [
            '使用 ROW_NUMBER() 分别按正序和倒序取第一条',
            'PARTITION BY session_id',
            '使用 MAX(CASE WHEN ...) 聚合获取首尾页面',
            'COUNT(*) 统计总事件数'
        ],
        'sampleAnswer': 'WITH ranked AS (\n  SELECT *,\n    ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time) AS rn_first,\n    ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time DESC) AS rn_last\n  FROM clickstream\n)\nSELECT\n  session_id,\n  MAX(CASE WHEN rn_first = 1 THEN page_url END) AS entry_page,\n  MAX(CASE WHEN rn_last = 1 THEN page_url END) AS exit_page,\n  COUNT(*) AS total_events\nFROM ranked\nGROUP BY session_id'
    },
]

data = {
    'componentId': 'spark-sql',
    'questions': question_data
}

with open('data/spark-sql-writing.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f'Created {len(question_data)} questions')
print(f'File size: {os.path.getsize("data/spark-sql-writing.json")} bytes')

# Verify
diff_counts = {'easy': {'writing': 0}, 'medium': {'writing': 0}, 'hard': {'writing': 0}}
for q in question_data:
    diff_counts[q['difficulty']]['writing'] += 1
for d, types in diff_counts.items():
    for t, c in types.items():
        if c > 0:
            print(f'  {d} {t}: {c}')
