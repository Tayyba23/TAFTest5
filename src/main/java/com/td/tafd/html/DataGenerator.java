/**
 * 
 */
package com.td.tafd.html;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.td.tafd.parsers.logfile.LogFileParser;
import com.td.tafd.vo.ChartColumnInfo;
import com.td.tafd.vo.ModuleTimeInfo;
import com.td.tafd.vo.PieChartInfo;

/**
 * @author mr255048
 *
 */
public class DataGenerator {

	public String generateHTML(List<ChartColumnInfo> columnInfoList,
			List<ModuleTimeInfo> rows, ChartType [] types, final String title,
			final String subTitle, final int width, final int height)
			throws IOException {
		String html = null;
		String divName = "line_chart";
		StringBuilder buffer = new StringBuilder();
		buffer.append("<html> \n <head>\n");
		buffer.append("<script type=\"text/javascript\" src=\"https://www.gstatic.com/charts/loader.js\"></script>\n");
		
		LogFileParser lfp = new LogFileParser();
		List<PieChartInfo> pci = lfp.getPassFailCounts();
		
		for(ChartType type : types)
		{
			switch (type) {
			case Pie:
				divName = "pie_chart";

				// before calling getPieChart, define and populate columnInfoList variable - remove it from generateHTML function's parameters
				// similarly specify title and subtitle etc.

				String [] tables = new String [] {"Sum Avg", "Min Max", "History"};
				
				int i=1;
				for(PieChartInfo info : pci)
				{
					buffer.append(getPieChart(info, tables[i-1], subTitle,
							300, 300, divName+i));
					++i;
				}
				
				break;
			case Gant:
			case Column:
				buffer.append(" <script type=\"text/javascript\"> \n");
				divName = "column_chart";
				buffer.append(getColumnChart(width, height, divName));
				break;
			case Combo:
			case Bar:
			case Donut:
			case Gauge:
			case Line:
				buffer.append(" <script type=\"text/javascript\"> \n");
				divName = "line_chart";
				buffer.append(getLineChart(columnInfoList, rows, title, subTitle,
						width, height, divName));
				break;
			case LineTopX:
				buffer.append(" <script type=\"text/javascript\"> \n");
				divName = "line_chart";

				// before calling getTopXLineChart, define and populate columnInfoList variable - remove it from generateHTML function's parameters

				buffer.append(getTopXLineChart(title, subTitle,
						700, height, divName));
				break;
			}
		}

		buffer.append("\n</head>\n<body><h1>                          Test Automation Framework Results (Latest Test-Cycle)</h1>\n");
		buffer.append("<div id=\"line_chart\" style=\"width: 900px; float: left;\">\n</div>\n");
		buffer.append("<div id=\"column_chart\" style=\"margin-left: 10px; width: 300px; float: left;\">\n</div>\n");
		buffer.append("<div id=\"pie_chart1\" style=\"width: 300px; float: left;\">\n</div>\n");
		buffer.append("<div id=\"pie_chart2\" style=\"margin-left: 10px; width: 300px; float: left;\">\n</div>\n");
		buffer.append("<div id=\"pie_chart3\" style=\"margin-left: 10px; width: 300px; float: left;\">\n</div>\n");
		buffer.append("</body>\n </html>");

		html = buffer.toString();

		File file = new File("report.html");
		FileUtils.write(file, html);
		return html;
	}

	private String getPieChart(PieChartInfo pci, final String title,
			final String subTitle, final int width, final int height,
			final String divName) {
		String script = null;
		StringBuilder buffer = new StringBuilder();
		buffer.append(" <script type=\"text/javascript\"> \n");
		buffer.append(" google.charts.load('current', {'packages':['corechart']});");
		buffer.append(" google.charts.setOnLoadCallback(drawChart);");
		buffer.append(" function drawChart() {" + "\n");
		buffer.append("var data = new google.visualization.DataTable();\n");
		
		List<ChartColumnInfo> columnInfoList = new ArrayList<ChartColumnInfo>();
		
		ChartColumnInfo table = new ChartColumnInfo();
		table.setDataType("string");
		table.setName("name");
		columnInfoList.add(table);
		
		ChartColumnInfo pass = new ChartColumnInfo();
		pass.setDataType("number");
		pass.setName("percentage");
		columnInfoList.add(pass);
		
		for (ChartColumnInfo cci : columnInfoList) {
			buffer.append("data.addColumn('" + cci.getDataType() + "','"
					+ cci.getName() + "');" + "\n");
		}

		buffer.append(" data.addRows([" + "\n");
		
		buffer.append("['Passed', " + pci.getPassPercentage() + "],\n");
		buffer.append("['Failed', " + pci.getFailPercentage() + "]");			
		buffer.append("\n");	

		buffer.append("]);\n");
		buffer.append("var options = { \n chart: { \n title: '" + title
				+ "',\n");
		buffer.append("subtitle: '" + subTitle + "' },\n width: " + width
				+ ",\n height: " + height);
		buffer.append(" };\n");
		buffer.append(" var chart = new google.visualization.PieChart(document.getElementById('"
				+ divName + "'));\n");
		buffer.append(" chart.draw(data, options);\n}\n");
		buffer.append("</script> ");

		script = buffer.toString();
		return script;
	}

	private String getColumnChart(final int width, final int height, final String divName)
	{
		String script = null;
		StringBuilder buffer = new StringBuilder();
		
		buffer.append("google.charts.load(\"current\", {packages:['corechart']}); \n");
		buffer.append("google.charts.setOnLoadCallback(drawChart); \n");
		buffer.append("function drawChart() { \n");
		buffer.append("var data = google.visualization.arrayToDataTable([ \n");
		buffer.append("[\"Element\", \"Percentage\", { role: \"style\" } ], \n");
		buffer.append("[\"Not Run\", 12.7, \"#CFDC6C\"], \n");
		buffer.append("[\"Unsuccessful\", 17.8, \"#DC9B6C\"], \n");
		buffer.append("[\"Successful\", 69.5, \"color: #69C761\"] \n");
		buffer.append("]); \n");

		buffer.append("var view = new google.visualization.DataView(data); \n");
		buffer.append("view.setColumns([0, 1, \n");
		buffer.append("{ calc: \"stringify\", \n");
		buffer.append("sourceColumn: 1, \n");
		buffer.append("type: \"string\", \n");
		buffer.append("role: \"annotation\" }, \n");
		buffer.append("2]); \n");

		buffer.append("var options = { \n");
		buffer.append("title: \"Module Success Percentage\", \n");
		buffer.append("width: 600, \n");
		buffer.append("height: 500, \n");
		buffer.append("bar: {groupWidth: \"95%\"}, \n");
		buffer.append("legend: { position: \"none\" }, \n");
		buffer.append("}; \n");
		buffer.append("var chart = new google.visualization.ColumnChart(document.getElementById('" + divName+ "')); \n");
		buffer.append("chart.draw(view, options); \n");
		buffer.append("} \n");
		buffer.append("</script> \n");
		
		script = buffer.toString();
		return script;
		
	}
	
	private String getLineChart(List<ChartColumnInfo> columnInfoList,
			List<ModuleTimeInfo> rows, final String title,
			final String subTitle, final int width, final int height,
			final String divName) {
		String script = null;
		StringBuilder buffer = new StringBuilder();
		buffer.append(" google.charts.load('current', {'packages':['line']});");
		buffer.append(" google.charts.setOnLoadCallback(drawChart);");
		buffer.append(" function drawChart() {" + "\n");
		buffer.append("var data = new google.visualization.DataTable();\n");
		for (ChartColumnInfo cci : columnInfoList) {
			buffer.append("data.addColumn('" + cci.getDataType() + "','"
					+ cci.getName() + "');" + "\n");
		}
		Iterator<ModuleTimeInfo> it = rows.iterator();
		buffer.append(" data.addRows([" + "\n");
		while (it.hasNext()) {
			ModuleTimeInfo mti = it.next();

			buffer.append("['" + mti.getModuleName() + "',"
					+ mti.getExecutionTime() + "]");
			if (it.hasNext()) {
				buffer.append(",");
			}
			buffer.append("\n");
		}
		buffer.append("]);\n");
		buffer.append("var options = { \n chart: { \n title: '" + title
				+ "',\n");
		buffer.append("subtitle: '" + subTitle + "' },\n width: " + width
				+ ",\n height: " + height + ",");
		buffer.append("  axes: {" + "\n");
		buffer.append(" x: {" + "\n");
		buffer.append("0: {side: 'top'}" + "\n");
		buffer.append("}" + "\n");
		buffer.append("}\n");
		buffer.append(" };\n");
		buffer.append("var chart = new google.charts.Line(document.getElementById('"
				+ divName + "'));\n");
		buffer.append(" chart.draw(data, google.charts.Line.convertOptions(options));\n}\n");
		buffer.append("</script> ");

		script = buffer.toString();
		return script;
	}

	private String getTopXLineChart(final String title,
			final String subTitle, final int width, final int height,
			final String divName) {
		String script = null;
		StringBuilder buffer = new StringBuilder();
		
		buffer.append("google.charts.load('current', {'packages':['line']});\n");
		buffer.append("google.charts.setOnLoadCallback(drawChart);\n");

		buffer.append("function drawChart() {\n");

		buffer.append("var data = new google.visualization.DataTable();\n");
		
		List<ChartColumnInfo> columnInfoList = new ArrayList<>();
		ChartColumnInfo name = new ChartColumnInfo();
		name.setDataType("number");
		name.setName("Module");
		columnInfoList.add(name);
		
		ChartColumnInfo type = new ChartColumnInfo();
		type.setDataType("number");
		type.setName("source/target1");
		columnInfoList.add(type);

		ChartColumnInfo type2 = new ChartColumnInfo();
		type2.setDataType("number");
		type2.setName("source/target2");
		columnInfoList.add(type2);
		
		ChartColumnInfo type3 = new ChartColumnInfo();
		type3.setDataType("number");
		type3.setName("source/target3");
		columnInfoList.add(type3);
		
		for (ChartColumnInfo cci : columnInfoList) {
			buffer.append("data.addColumn('" + cci.getDataType() + "','"
					+ cci.getName() + "');" + "\n");
		}

		LogFileParser lfp = new LogFileParser();
		
		String [] moduleNo = new String [] {"6","7","8","9","10","11","12","13"};
		
		ArrayList<ModuleTimeInfo> table1ExecutionTimes = new ArrayList<ModuleTimeInfo>();
		ArrayList<ModuleTimeInfo> table2ExecutionTimes = new ArrayList<ModuleTimeInfo>();
		ArrayList<ModuleTimeInfo> table3ExecutionTimes = new ArrayList<ModuleTimeInfo>();
		
		lfp.tableWiseExecutionTimes(table1ExecutionTimes, table2ExecutionTimes, table3ExecutionTimes);
		
		System.out.println("\nSource/Target 1:\n");
		for(ModuleTimeInfo module : table1ExecutionTimes)
			System.out.println("Module Name: " + module.getModuleName() + ", Execution time: " + module.getExecutionTime());
		
		System.out.println("\nSource/Target 2:\n");
		for(ModuleTimeInfo module : table2ExecutionTimes)
			System.out.println("Module Name: " + module.getModuleName() + ", Execution time: " + module.getExecutionTime());
		
		System.out.println("\nSource/Target 3:\n");
		for(ModuleTimeInfo module : table3ExecutionTimes)
			System.out.println("Module Name: " + module.getModuleName() + ", Execution time: " + module.getExecutionTime());
		
		buffer.append("data.addRows([\n");
		
		for(int i=0; i< table1ExecutionTimes.size(); ++i)
		{
			buffer.append("[" + moduleNo[i] + ",   " 
					+ table1ExecutionTimes.get(i).getExecutionTime() 
					+ ", " + table2ExecutionTimes.get(i).getExecutionTime() 
					+ ",  " + table3ExecutionTimes.get(i).getExecutionTime() 
					+ "],\n");
		}
		
		buffer.append("]);\n");

		buffer.append("var options = {\n");
		buffer.append("chart: {\n");
		buffer.append("title: 'Module Execution Times',\n");
		buffer.append("subtitle: 'Source/Target pair-wise'\n");
		buffer.append("},\n");
		buffer.append("width: 900,\n");
		buffer.append("height: 500,\n");
		buffer.append("axes: {\n");
		buffer.append("x: {\n");
		buffer.append("0: {side: 'bottom'}\n");
		buffer.append("}\n");
		buffer.append("}\n");
		buffer.append("};\n");

		buffer.append("var chart = new google.charts.Line(document.getElementById('" + divName + "'));\n");

		buffer.append("chart.draw(data, google.charts.Line.convertOptions(options));\n");
		buffer.append("}\n\n");
		buffer.append("</script> ");

		script = buffer.toString();
		return script;
	}
	
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		
		//new JobTypeParser();
		String title = "Name of the module";
		String subTitle = "Execution time";
		int width = 900;
		int height = 500;
		List<ChartColumnInfo> columnInfoList = new ArrayList<>();
				
		List<ModuleTimeInfo> rows = new ArrayList<>();

		ModuleTimeInfo duplicateRecord = new ModuleTimeInfo("Passed",
				63);
		rows.add(duplicateRecord);
		ModuleTimeInfo ri = new ModuleTimeInfo("Failed", 34);
		rows.add(ri);
		
		ChartType [] types = new ChartType[3];
		types[0] = ChartType.LineTopX;
		types[1] = ChartType.Pie;
		types[2] = ChartType.Column;
		
		String html = new DataGenerator().generateHTML(columnInfoList, rows,
				types, title, subTitle, width, height);

		System.out.println(html);
	}

}
