import pyspark
from pyspark.sql import SparkSession
import time
from datetime import datetime
import os


def execute_mysql_query(spark, jdbc_url, user, password, query):
    """使用Spark的JVM接口执行MySQL查询"""
    jvm = spark._jvm

    # 显式加载MySQL驱动
    try:
        jvm.Class.forName("com.mysql.cj.jdbc.Driver")
    except Exception as e:
        print(f"加载MySQL驱动失败: {str(e)}")
        raise

    # 获取数据库连接 trythis
    connection = jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
    connection.setAutoCommit(False)

    try:
        # 创建并执行语句
        statement = connection.createStatement()
        statement.execute(query)

        # 提交事务
        connection.commit()
        return {"success": True, "message": "SQL执行成功"}

    finally:
        # 确保资源关闭
        statement.close()
        connection.setAutoCommit(True)
        connection.close()


def run_periodic_queries(jdbc_url, user, password, query, interval=30):
    """按指定间隔周期性执行查询"""
    # 获取当前脚本所在目录
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # 配置SparkSession - 添加MySQL驱动
    spark = SparkSession.builder \
        .appName("MySQLPeriodicQuery") \
        .config("spark.jars", f"file:///{current_dir}/mysql-connector-j-8.0.33.jar".replace("\\", "/")) \
        .getOrCreate()

    # 设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    try:
        while True:
            start_time = datetime.now()
            print(f"开始执行查询: {start_time}")

            result = execute_mysql_query(spark, jdbc_url, user, password, query)
            end_time = datetime.now()

            duration = (end_time - start_time).total_seconds()
            print(f"查询完成: {end_time}，耗时: {duration:.2f}秒")
            print(f"执行结果: {result['message']}")

            # 计算等待时间
            wait_time = max(0, interval - duration)
            if wait_time > 0:
                print(f"等待{wait_time:.2f}秒后再次执行")
                time.sleep(wait_time)

    except KeyboardInterrupt:
        print("程序被用户中断")
    except Exception as e:
        print(f"发生错误: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    # 数据库连接配置 - 使用root数据库和root用户
    jdbc_url = "jdbc:mysql://localhost:3306/root?useSSL=false&serverTimezone=UTC"
    user = "root"
    password = "root"

    # 修正后的SQL查询语句
    sql_query = """
    INSERT INTO ads (user_id, date_str, pv, uv, click_cnt, spend)
    SELECT 
        user_id,
        DATE(STR_TO_DATE(hour_str, '%Y%m%d%H')) AS date_str,  -- 将hour_str转换为日期
        SUM(pv) AS pv,
        SUM(uv) AS uv,
        SUM(click_cnt) AS click_cnt,
        SUM(spend) AS spend
    FROM dws_detail
    GROUP BY user_id, DATE(STR_TO_DATE(hour_str, '%Y%m%d%H'))
    ON DUPLICATE KEY UPDATE
        pv = VALUES(pv),
        uv = VALUES(uv),
        click_cnt = VALUES(click_cnt),
        spend = VALUES(spend);
    """

    print("启动周期性MySQL查询任务")
    print(f"数据库: {jdbc_url}")
    print(f"用户: {user}")
    print(f"查询间隔: 30秒")
    print("按 Ctrl+C 停止程序")

    # 启动周期性查询
    run_periodic_queries(jdbc_url, user, password, sql_query, interval=10)