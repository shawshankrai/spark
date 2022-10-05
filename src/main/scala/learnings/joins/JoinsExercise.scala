package learnings.joins

import org.apache.spark.sql.functions.max
import utils.SparkUtils
object JoinsExercise extends App {
  /**
   * Exercise
   * - show all employees and their max salary
   * - show all employees who were never managers
   * - find the job title of best paid 10 employees
   * */

  val spark = SparkUtils.getSparkSession("Join_Exercise-1", "local")
  import spark.implicits._

  val employeesDF = SparkUtils.getDataFrameFromDB(spark, "employees")
  val salariesDF = SparkUtils.getDataFrameFromDB(spark, "salaries")
  val deptManagerDF =  SparkUtils.getDataFrameFromDB(spark, "dept_manager")
  val titlesDF = SparkUtils.getDataFrameFromDB(spark, "titles")

  // 1
  val maxSalaryByEmployeeId = salariesDF.groupBy($"emp_no").max("salary")
  val employeesWithMaxSalary = employeesDF.join(maxSalaryByEmployeeId, Seq("emp_no"), "left")
  employeesWithMaxSalary.show()

  // 2
  val employeesWhoWereNeverManagers = employeesDF.join(deptManagerDF, Seq("emp_no"), "left_anti")
  employeesWhoWereNeverManagers.show()
  // validation
  deptManagerDF.join(employeesWhoWereNeverManagers, "emp_no").show()

  // 3
  val EmployeesCurrentRoleDate = titlesDF.groupBy("emp_no").agg(max("from_date"))
  EmployeesCurrentRoleDate.show()

  val currentTitleByDate = EmployeesCurrentRoleDate.join(titlesDF, Seq("emp_no")).where($"max(from_date)" === $"from_date")
  currentTitleByDate.show()

  val topTenEmployeesBasedOnSalary = employeesWithMaxSalary.selectExpr("*").orderBy($"max(salary)".desc).limit(10)

  val topTenEmployeesBasedOnSalaryTitles = topTenEmployeesBasedOnSalary.join(currentTitleByDate, Seq("emp_no"), "left")
  topTenEmployeesBasedOnSalaryTitles.show()

  // Validation
  topTenEmployeesBasedOnSalaryTitles.filter($"max(from_date)" =!= $"from_date").show()
}
