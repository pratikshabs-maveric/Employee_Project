import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class DBConnection {
	
	static String url = "jdbc:sqlserver://MAVCHN0522236\\SQLEXPRESS;databaseName=EmployeeData;integratedSecurity=true;"
			+ "encrypt=false;TrustServerCertificate=true;";
	
	public static void insertDB(EmployeeDetails ed,boolean flag)
	{
		try
		{
			String query="";
			if(flag==true)
			{
				query = "Insert into Employee_Data values('"+ed.getEmployee_Id()+"','"
						+ed.getEmployee_Name()+"','"+ed.getEmail()+"','"+ed.getPhone_number()+"','"
						+ed.getHire_Date()+"','"+ed.getJob_Id()+"','"+ed.getSalary()+"','"
						+ed.getCommission_Pct()+"','"+ed.getManager_Id()+"','"+ed.getDepartment_Id()+"',"+"10-12-22"+")";
			}
			else
			{
				query = "Insert into Employee_Failed values('"+ed.getEmployee_Id()+"','"
						+ed.getEmployee_Name()+"','"+ed.getEmail()+"','"+ed.getPhone_number()+"','"
									+ed.getHire_Date()+"','"+ed.getJob_Id()+"','"+ed.getSalary()+"','"
						+ed.getCommission_Pct()+"','"+ed.getManager_Id()+"','"+ed.getDepartment_Id()+"',"+"10-12-22"+")";
			}
			System.out.println("Insert Query ->"+query);
//			Class.forName("com.mysql.cj.jdbc.Driver");
			try(Connection conn = DriverManager.getConnection(url))
				{
				PreparedStatement stmt = conn.prepareStatement(query);
				stmt.executeUpdate();
				System.out.println("Insertion Successfull!");
				conn.close();
				}
			catch(SQLException e)
			{
			System.out.println("Connection failed!");
			e.printStackTrace();
			}
			
		}
		catch(Exception e)
		{
		System.out.println("Connection failed!");
		e.printStackTrace();
		}
	}
	
	public static List<EmployeeReport> getReport(boolean flag) {
		List<EmployeeReport> erList = new ArrayList<EmployeeReport>();
		try
		{
			String query="";
			if(flag==true)
			{
				query = "SELECT emp.employee_Id,emp.employee_Name,emp.email,empj.job_description,[dbo].[get_manager_name]"
						+ "(emp.manager_Id) as ManagerName,dept.department_desc from Employee_Data emp LEFT JOIN "
						+ "Employee_Job empj ON emp.job_Id = empj.job_Id Left Join Employee_Department dept on "
						+ "emp.department_Id=dept.department_Id";
			}
			else
			{
				query = "SELECT emp.employee_Id,[dbo].[get_emp_name](emp.employee_Id) as EmployeeName,emp.email,"
						+ "empj.job_description,[dbo].[get_manager_name](emp.manager_Id) as ManagerName,"
						+ "dept.department_desc from Employee_Failed emp Left join Employee_Data empf on "
						+ "emp.employee_Id=empf.employee_Id LEFT JOIN Employee_Job empj ON emp.job_Id = empj.job_Id "
						+ "Left Join Employee_Department dept on emp.department_Id=dept.department_Id";
			}
			try(Connection conn = DriverManager.getConnection(url))
				{
				
				System.out.println("Connection Successfull!");
				System.out.println("This is query " + query);
				Statement stmt = conn.createStatement();
				ResultSet result = stmt.executeQuery(query);
				while(result.next())
				{
					EmployeeReport er = new EmployeeReport();
					er.setEmployee_Id(result.getString(1));
					er.setEmployee_Name(result.getString(2));
					er.setEmail(result.getString(3));
					er.setJob_description(result.getString(4));
					er.setManager_Name(result.getString(5));
			
					erList.add(er);
				}
				
				conn.close();
				}
		
		}
		catch(SQLException e)
		{
		System.out.println("Connection failed!");
		e.printStackTrace();
		}
		return erList;
	}
	
	public static void emptyDB()
	{
		String query1 = "Delete from Employee_Data";
		String query2 = "Delete from Employee_Failed";
		try(Connection conn = DriverManager.getConnection(url))
		{
			PreparedStatement stmt = conn.prepareStatement(query1);
			stmt.executeUpdate();
			System.out.println("Old Records from Employee_Data deleted!");
			stmt = conn.prepareStatement(query2);
			stmt.executeUpdate();
			System.out.println("Old Records from Employee_Failed deleted!");
			conn.close();
		}
		catch(SQLException e)
		{
			System.out.println("Connection failed!");
			e.printStackTrace();
		}
	}
}
