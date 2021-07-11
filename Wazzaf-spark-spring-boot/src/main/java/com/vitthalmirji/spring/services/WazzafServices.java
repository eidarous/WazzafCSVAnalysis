/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vitthalmirji.spring.services;

import java.awt.Color;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.BitmapEncoder.BitmapFormat;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;

/**
 * @author Mahmoud Eidarous
 */
public class WazzafServices {
    
    public List<String> dfToList(Dataset<Row> df){
         List results = new ArrayList();
        
        for(Row r:df.collectAsList()){
            results.add(r);
        }
        List<String> label = new ArrayList<>();
        for(int i = 0; i< results.size(); i++){
            label.add((String)((Row)results.get(i)).toString()); 
        }
        return label;
    }
    
    public String readAndView(Dataset<Row> df){
        
        List results = new ArrayList();
        
        for(Row r:df.collectAsList()){
            results.add(r);
        }
        List<String> label = new ArrayList<>();
        for(int i = 0; i< 40; i++){
            label.add((String)((Row)results.get(i)).toString()); 
        }
        
        
        String html = String.format("<h1 style=\"text-align:center;font-family:verdana;background-color:SpringGreen;\">%s</h1>", "Sample From The Data") +
                "<table style=\"width:100%;text-align: center\">" +
                "<tr><th>Title</th><th>Company</th><th>Location</th><th>Type</th><th>Level</th><th>YearsExp</th><th>Country</th><th>Skills</th></tr>";
        
        for(int i = 0; i< 40; i++){
            String[] row = label.get(i).replace("[", "").replace("]", "").split(",");
            html += "<tr>\n" +"<td>"+row[0]+"</td>\n" +"<td>"+row[1]+"</td>\n" +"<td>"+row[2]+"</td>\n"
                    +"<td>"+row[3]+"</td>\n" +"<td>"+row[4]+"</td>\n" +"<td>"+row[5]+"</td>\n"+"<td>"+row[6]+"</td>\n"+"<td>"+((String)((Row)results.get(i)).get(7).toString())+"</td>\n"+"  </tr>";
            
        }
        
        html += "</table>";
        
        return html;
    }
    
    public String schemaAndSummary(Dataset<Row> df){
        String html = String.format("<h1 style=\"text-align:center;font-family:verdana;background-color:SpringGreen;\">%s</h1>", "Schema And Summary For The Data") +
                String.format("<h2 style=\"text-align:center;\"> Total records = %d</h2>", df.count())+ 
                String.format("<h2 style=\"text-align:center;\"> Schema </h2>")+
                "<table style=\"width:100%;text-align: center\">" ;
        String[] st = df.schema().toDDL().split(",");
        
        for (String st1 : st) {
           html += "<tr><td><br>"+ st1 +"</tr></td>" ;
        }
        
    return html;
    }
    
    public String clean(Dataset<Row> df){
        Dataset<Row> wazzafUniqeRec =df.distinct();
        Dataset<Row> wazzafNoNull=wazzafUniqeRec.filter("YearsExp NOT LIKE 'null%'");
        String html = String.format("<h1 style=\"text-align:center;font-family:verdana;background-color:SpringGreen;\">%s</h1>", "Cleaning Process Summary") +
                String.format("<h2 style=\"text-align:center;\">Total records Before Cleaning = %d</h2>", df.count()) +
                String.format("<h2 style=\"text-align:center;\">Number Of Duplicate Records = %d</h2>", df.count()-wazzafUniqeRec.count()) +
                String.format("<h2 style=\"text-align:center;\">Number Of Records Have Null Values = %d</h2>", wazzafUniqeRec.count()-wazzafNoNull.count()) +
                String.format("<h2 style=\"text-align:center;\">Total records After Cleaning = %d</h2>", wazzafNoNull.count());
        return html ;
    }
    
    public Dataset<Row> calculate_Frequency(Dataset<Row> df ,String ColName){
        
       Dataset<Row> result = df.groupBy(ColName).agg(functions.count(ColName).as("Count")).sort(functions.desc("Count"));
        return result ;
        }
    
    public String jopsPerCompany( Dataset<Row> df){
        Dataset<Row> wazzafClean =df.distinct().filter("YearsExp NOT LIKE 'null%'");
        Dataset<Row> companyCount = calculate_Frequency(wazzafClean,"Company");
        List<String> label =dfToList(companyCount);
        
        String html = String.format("<h1 style=\"text-align:center;font-family:verdana;background-color:SpringGreen;\">%s</h1>", "What are the most demanding companies for jobs?") +
                "<table style=\"width:75%;text-align:center\">" +
                "<tr ><th>Company</th><th>JopsCount</th></tr>";
        
        for(int i = 0; i< 40; i++){
            String[] row = label.get(i).replace("[", "").replace("]", "").split(",");
            html += "<tr>\n" +"<td>"+row[0]+"</td>\n" +"<td>"+row[1]+"</td>\n" +"</td>\n"+"</tr>";
        }
        
        html += "</table>";
        
        return html;
    }
    
    public String popularTitle( Dataset<Row> df){
        Dataset<Row> wazzafClean =df.distinct().filter("YearsExp NOT LIKE 'null%'");
        Dataset<Row> titleFreq = calculate_Frequency(wazzafClean,"Title");
        List<String> label =dfToList(titleFreq);
        
        String html = String.format("<h1 style=\"text-align:center;font-family:verdana;background-color:SpringGreen;\">%s</h1>", "What are the most popular job titles?") +
                "<table style=\"width:80%;text-align:center\">" +
                "<tr ><th>Title</th><th>TitleCount</th></tr>";
        
        for(int i = 0; i< 40; i++){
            String[] row = label.get(i).replace("[", "").replace("]", "").split(",");
            html += "<tr>\n" +"<td>"+row[0]+"</td>\n" +"<td>"+row[1]+"</td>\n" +"</td>\n"+"</tr>";
        }
        
        html += "</table>";
        
        return html;
    }
    
    public String popularCities( Dataset<Row> df){
        Dataset<Row> wazzafClean =df.distinct().filter("YearsExp NOT LIKE 'null%'");
        Dataset<Row> citiesFreq = calculate_Frequency(wazzafClean,"Location");
        List<String> label =dfToList(citiesFreq);
        
        String html = String.format("<h1 style=\"text-align:center;font-family:verdana;background-color:SpringGreen;\">%s</h1>", "What are the most popular areas?") +
                "<table style=\"width:85%;text-align:center\">" +
                "<tr ><th>Area</th><th>AreaCount</th></tr>";
        
        for(int i = 0; i< 40; i++){
            String[] row = label.get(i).replace("[", "").replace("]", "").split(",");
            html += "<tr>\n" +"<td>"+row[0]+"</td>\n" +"<td>"+row[1]+"</td>\n" +"</td>\n"+"</tr>";
        }
        
        html += "</table>";
        
        return html;
    }
    
    public String neededSkills( Dataset<Row> df){
        Dataset<Row> wazzafClean =df.distinct().filter("YearsExp NOT LIKE 'null%'");
        
        List skillsVal = new ArrayList<>();
        for(Row r:wazzafClean.select("Skills").collectAsList()){
            skillsVal.add(r);
        }
        
        List<String> skillsSplited = new ArrayList<>();
        for(int i = 0; i< skillsVal.size(); i++){
            skillsSplited.addAll(Arrays.asList(((String)((Row)skillsVal.get(i)).get(0)).split(","))); 
        }
      
        Map<String, Long> frequencyMap =skillsSplited.stream().collect(Collectors.groupingBy(Function.identity(),Collectors.counting()));
 
        
        //LinkedHashMap preserve the ordering of elements in which they are inserted
        LinkedHashMap<String, Long> reverseSortedMap = new LinkedHashMap<>();

        //Use Comparator.reverseOrder() for reverse ordering
        frequencyMap.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())) 
            .forEachOrdered(x -> reverseSortedMap.put(x.getKey(), x.getValue()));
        
        String html = String.format("<h1 style=\"text-align:center;font-family:verdana;background-color:SpringGreen;\">%s</h1>", "What are the most Needed Skills?") +
                "<table style=\"width:70%;text-align:center\">" +
                "<tr><th>Skill</th><th >Count</th></tr>";
        
        
        ArrayList<Object> l1 = new ArrayList<>();
  
        ArrayList<Object> l2 = new ArrayList<>();
  
        for (Map.Entry<String, Long> it : reverseSortedMap.entrySet()) {
            l1.add(it.getKey());
            l2.add(it.getValue());
        }
        
        for(int i = 0; i< 100; i++){
          
            html += "<tr>\n" +"<td>"+l1.get(i)+"</td>\n" +"<td>"+l2.get(i)+"</td>\n" +"</td>\n"+"</tr>";
        }
        
        html += "</table>";
   
        return html ;
    }
    
    public String jopsPerCompChart(Dataset<Row> df) throws IOException{
        Dataset<Row> wazzafClean =df.distinct().filter("YearsExp NOT LIKE 'null%'");
        Dataset<Row> companyCount = calculate_Frequency(wazzafClean,"Company");
        chart(companyCount,"pie chart","Jops Per Company","",""); 
        String html = String.format("<h1>%s</h1>", "Jops Per Company Pie Chart") +
                String.format("<h5>The Chart<br/></h5> ")+
                String.format("<img src=\"%s\">","/pie.jpg");
        return html;
    }
    
    public String titleChart(Dataset<Row> df) throws IOException{
        Dataset<Row> wazzafClean =df.distinct().filter("YearsExp NOT LIKE 'null%'");
        Dataset<Row> titleFreq = calculate_Frequency(wazzafClean,"Title");
        chart(titleFreq,"bar chart","Most Popular Jop Titels","Jop Titels","Count") ;
        String html = String.format("<h1>%s</h1>", "Most Popular Jop Titels Bar Chart") +
                String.format("<img src=\"%s\">","/titlebar.jpg");
        return html;
    }
    
    public String areaChart(Dataset<Row> df) throws IOException{
        Dataset<Row> wazzafClean =df.distinct().filter("YearsExp NOT LIKE 'null%'");
        Dataset<Row> citiesFreq = calculate_Frequency(wazzafClean,"Location");
        chart(citiesFreq,"bar chart","Most Popular Cities","Cities","Count");
        String html = String.format("<h1>%s</h1>", "Most Popular Areas Bar Chart") +
                String.format("<img src=\"%s\">","/areabar.jpg");
        return html;
    }   
    
    public void chart(Dataset<Row> df ,String typeChart,String title,String xtitle,String ytitle) throws IOException {
        List results = new ArrayList();
        
        for(Row r:df.collectAsList()){
            results.add(r);
        }
        List<String> label = new ArrayList<>();
        for(int i = 0; i< 4; i++){
            label.add((String)((Row)results.get(i)).get(0)); 
        }
        List<Long> count = new ArrayList<>();
        for(int i = 0; i< 4; i++){
            count.add( (long)((Row)results.get(i)).get(1)); 
        }
        
       
        String chartType = typeChart;
        if (chartType == "bar chart")
        {
        
         // Create Chart
        CategoryChart chart = new CategoryChartBuilder().width(800).height(600).title(title).xAxisTitle(xtitle).yAxisTitle(ytitle).build();

        // Customize Chart
//        chart.getStyler().setLegendPosition(LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setStacked(true);
        
        

        // Series
        chart.addSeries(" ",label,count);
        
        //show
        
        BitmapEncoder.saveBitmapWithDPI(chart,"./sample", BitmapFormat.JPG,300);
        
        }
        else if (chartType == "pie chart"){
                // Create Chart
            PieChart jopsCompany = new PieChartBuilder().width(800).height(600).title(title).build();

            // Customize Chart
            Color[] sliceColors = new Color[] { new Color(224, 68, 14), new Color(230, 105, 62), new Color(236, 143, 110), new Color(243, 180, 159), new Color(246, 199, 182) };
            jopsCompany.getStyler().setSeriesColors(sliceColors);

            // Series
            for(int i = 1; i< 6; i++){
                jopsCompany.addSeries(label.get(i), count.get(i));
            }
       
        BitmapEncoder.saveBitmapWithDPI(jopsCompany, "./sample2", BitmapFormat.JPG,300);

       
        }
        else{
            System.out.println("Not Suporrted Chart");
        }
    }
}
