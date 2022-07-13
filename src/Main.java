import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {
	
	public static void main(String args[])
	  {
		File file = new File("../Employee_Data.txt");
		File success_file = new File("../Success_Report.txt");
		File failure_file = new File("../Failure_Report.txt");
		try {
			Scanner sc = new Scanner(file);
			sc.nextLine();
			while (sc.hasNextLine())
			{
				insertDB(sc.nextLine());
			}
		} catch (FileNotFoundException e) {
			System.out.println("Exception->"+e);
			e.printStackTrace();
		}
		
		try
		{
			List<EmployeeReport> success_report = DBConnection.getReport(true);
			List<EmployeeReport> failed_report = DBConnection.getReport(false);
				
			BufferedWriter out = new BufferedWriter(new FileWriter(success_file));
			out.write("EmployeeID,Name,Email,Job Description,Department Name,Manager Name");
			out.newLine();

			for(EmployeeReport er : success_report)
			{
				out.write(er.getEmployee_Id()+","+er.getEmployee_Name()+","+er.getEmail()+","
						+ ""+er.getJob_description()+","
						+ ""+er.getDepartment_Name()+","+er.getManager_Name());
				out.newLine();
			}
			out.close();
			System.out.println("Success Report generated!");
			
			out = new BufferedWriter(new FileWriter(failure_file));
			out.write("EmployeeID,Name,Email,Job Description,Department Name,Manager Name");
			out.newLine();
			for(EmployeeReport er : failed_report)
			{
				System.out.println(er.getEmployee_Id() + " In the failed report");
				out.write(er.getEmployee_Id()+","+er.getEmployee_Name()+","+er.getEmail()+","+er.
						getJob_description()+","+er.getDepartment_Name()+","+er.getManager_Name());
				out.newLine();
			}
			out.close();
			System.out.println("Failure Report generated!");

		}
		catch(Exception e)
		{
			System.out.println("Exception->"+e);
		}
	  }

	public static void insertDB(String record)
	{
		record = record.replaceAll(",,",",-,");
		record = record.replaceAll(",,",",-,");
		if(record.charAt(record.length()-1)==',')
		{
			record = record+"-";
		}
		
		String records[] = record.split(",");
		EmployeeDetails success_record = new EmployeeDetails();
		EmployeeDetails failure_record = new EmployeeDetails();
		
		//Employee_ID
		if(isNull(records[0]))
		{
			//inserts complete record into failure
			failure_record.setEmployee_Id(records[0]);
			failure_record.setEmployee_Name(records[1]);
			failure_record.setEmail(records[2]);
			failure_record.setPhone_number(records[3]);
			failure_record.setHire_Date(records[4]);
			failure_record.setJob_Id(records[5]);
			failure_record.setSalary(records[6]);
			failure_record.setCommission_Pct(records[7]);
			failure_record.setManager_Id(records[8]);
			failure_record.setDepartment_Id(records[9]);
			DBConnection.insertDB(failure_record, false);
		}
		else
		{
			success_record.setEmployee_Id(records[0]);
			failure_record.setEmployee_Id(records[0]);
		
			//Name
			if(isNameOnlyAlphabet(records[2]))
			{
				if(isNull(records[1]))
				{
					success_record.setEmployee_Name(records[2]);;
					failure_record.setEmployee_Name("-");
				}
				else
				{
					success_record.setEmployee_Name(records[1]+" "+records[2]);
					failure_record.setEmployee_Name("-");
				}
			}
			else
			{
				failure_record.setEmployee_Name(records[1]+" "+records[2]);
			}
			
			//Email
			String email = isValidEmail(records[3]);
			if(email.equalsIgnoreCase("false"))
			{
				failure_record.setEmail(records[3]);
				success_record.setEmail("-");
			}
			else
			{
				success_record.setEmail(email);
				failure_record.setEmail("-");
			}
			
			//PhoneNumber
			if(isValidPhoneNumber(records[4]))
			{
				success_record.setPhone_number(records[4]);
				failure_record.setPhone_number("-");
			}
			else
			{
				success_record.setPhone_number("-");
				failure_record.setPhone_number(records[4]);
			}
			
			//HireDate
			if(isValidDate(records[5]))
			{
				success_record.setHire_Date(records[5]);
				failure_record.setHire_Date("-");
			}
			else
			{
				success_record.setHire_Date("-");
				failure_record.setHire_Date(records[5]);
			}
			
			//JOBID
			if(isNull(records[6]))
			{
				success_record.setJob_Id("-");
				failure_record.setJob_Id("-");
			}
			else
			{
				success_record.setJob_Id(records[6]);
				failure_record.setJob_Id(records[6]);
			}
			
			//Salary
			if(isValidSalary(records[7]))
			{
				success_record.setSalary(records[7]);
				failure_record.setSalary("-");
			}
			else
			{
				failure_record.setSalary(records[7]);
				success_record.setSalary("-");
			}
			
			//COMMISSION_PCT
			success_record.setCommission_Pct(records[8]);
			failure_record.setCommission_Pct("-");
			
			//MANAGER_ID
			if(isNull(records[9]))
			{
				success_record.setManager_Id(records[0]);
				failure_record.setManager_Id(records[0]);
			}
			else
			{
				success_record.setManager_Id(records[9]);
				failure_record.setManager_Id(records[9]);
			}
			
			//DEPARTMENT_ID
			if(isNull(records[10]))
			{
				success_record.setDepartment_Id("-");
				failure_record.setDepartment_Id("-");
			}
			else
			{
				success_record.setDepartment_Id(records[10]);
				failure_record.setDepartment_Id(records[10]);
			}
			if(!(failure_record.getEmployee_Name().equalsIgnoreCase("-") && 
					failure_record.getEmail().equalsIgnoreCase("-")  && 
					failure_record.getPhone_number().equalsIgnoreCase("-") && 
					failure_record.getHire_Date().equalsIgnoreCase("-")
					 && failure_record.getSalary().equalsIgnoreCase("-")))
			{
				//Insert Failure record
				DBConnection.insertDB(failure_record, false);
			}
			//Insert Success record
			DBConnection.insertDB(success_record, true);
		}
		
		
	}
	
	public static boolean isNull(String value)
	{
		if(value.trim().equalsIgnoreCase("")||value.equals("null")||value.equals(null)||
				value.trim().equalsIgnoreCase("-"))
		{
			return true;
		}
		return false;
	}
	
	public static boolean isNameOnlyAlphabet(String value) 
	{
	    return ((!value.equals(""))
	            && (value != null)
	            && (value.matches("^[a-zA-Z ]*$")));
	}
	
	public static String isValidEmail(String value)
    {
		if(isNull(value))
		{
			return "false";
		}
		else
		{
			if(value.length()<=50)
			{
				if(isNameOnlyAlphabet(value))
				{
					return value+"@abc.com";
				}
		        String emailRegex = "^[a-zA-Z0-9_+&*-]+(?:\\."+
		                            "[a-zA-Z0-9_+&*-]+)*@" +
		                            "(?:[a-zA-Z0-9-]+\\.)+[a-z" +
		                            "A-Z]{2,7}$";
		                              
		        Pattern pat = Pattern.compile(emailRegex);
		        if(pat.matcher(value).matches())
		        {
		        	return value;
		        }
		        else
		        {
		        	return "false";
		        }
			}
			else
			{
				return "false";
			}
		}
    }
	
	public static boolean isValidPhoneNumber(String value)
	{
		if(isNull(value))
		{
			return false;
		}
		else
		{
			if(value.length()==12)
			{
				//"\\d{3}[-\\.\\s]\\d{3}[-\\.\\s]\\d{4}" pattern for the complete phoneNumber
				if(value.charAt(3)=='.' && value.charAt(7)=='.')
				{
					String phoneRegex ="^[\\.0-9]*$";
					Pattern pat = Pattern.compile(phoneRegex);
					return pat.matcher(value).matches();
				}
				else
				{
					return false;
				}
			}
			else
			{
				return false;
			}
		}
	}
	
	public static boolean isValidDate(String value)
    {
        String regex = "([0-2][0-9]||3[0-1])-(0[0-9]||1[0-2])-((19|20)\\d\\d)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher((CharSequence)value);
        return matcher.matches();
    }
	
	public static boolean isValidSalary(String value)
    {
		if(isNull(value))
		{
			return false;
		}
		else
		{
		String regex = "^([0-9]*)(.[[0-9]+]?)?$";
		Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher((CharSequence)value);
        return matcher.matches();
		}
    }
}
