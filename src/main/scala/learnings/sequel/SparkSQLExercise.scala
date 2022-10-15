package learnings.sequel

import utils.SparkSequelUtils.transferTables
import utils.SparkUtils.LOCAL
import utils.{PathGenerators, SparkUtils}

object SparkSQLExercise extends App {

  /**
   * save movies as spark table
   * Employees hired count between jan 1 2000 to jan 1 2001 hired date - employees table
   * Show avg salary from above grouped by dept_no - jain dept_emp and salary
   * Show name of best paying dept for employees 1999-2001
   * */

  val spark = SparkUtils.getSparkSession("Spark-Sequel-1", LOCAL)
  spark.sql("create database rtjvm") // Will bring already created db into session


  // 1
  spark.read.json(PathGenerators.getPathResourcesMainFolder("movies.json"))
    .write
    //.saveAsTable("rtjvm.movies_dup_exercise")

  // 2
  // Read table create temp view, do not write
  transferTables(spark, List("titles",
    "employees",
    "departments",
    "dept_manager",
    "dept_emp",
    "salaries")
    , shouldWritAsSparkTable = false)

  spark.sql("select count(*) from employees where hire_date > '1999-01-01' and hire_date < '2001-01-01'")
    .show()
  /**
   * +--------+
   * |count(1)|
   * +--------+
   * |     153|
   * +--------+
   */

  // 3
  spark.sql(
    """ select de.dept_no, avg(s.salary) from
      | employees e, dept_emp de, salaries s
      | where e.hire_date > '1999-01-01' and e.hire_date < '2001-01-01'
      | and e.emp_no = de.emp_no
      | and e.emp_no = s.emp_no
      | group by de.dept_no
      |""".stripMargin
  ).show()

  /**
   * +-------+------------------+
   * |dept_no|       avg(salary)|
   * +-------+------------------+
   * |   d005|51286.625806451615|
   * |   d009|        46715.9375|
   * |   d003|           49707.0|
   * |   d001| 64232.27272727273|
   * |   d007| 72266.44067796611|
   * |   d004|49321.143939393936|
   * |   d002|59013.708333333336|
   * |   d006|           46621.5|
   * |   d008|           54278.1|
   * +-------+------------------+
   *
   * */

  // 4 Non Aggregate columns in select and group by must be same
  spark.sql(
    """ select d.dept_name, d.dept_no, avg(s.salary) as payments from
      | employees e, dept_emp de, salaries s, departments d
      | where e.hire_date > '1999-01-01' and e.hire_date < '2001-01-01'
      | and e.emp_no = de.emp_no
      | and e.emp_no = s.emp_no
      | and de.dept_no = d.dept_no
      | group by d.dept_name, d.dept_no
      | order by payments desc
      | limit 1
      |""".stripMargin
  ).show()

  /**
   * +---------+-------+-----------------+
   * |dept_name|dept_no|         payments|
   * +---------+-------+-----------------+
   * |    Sales|   d007|72266.44067796611|
   * +---------+-------+-----------------+
   * */
}
