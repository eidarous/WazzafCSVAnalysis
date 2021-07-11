package com.vitthalmirji.spring;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import com.vitthalmirji.spring.services.WazzafServices;
import java.io.IOException;

@Controller
public class SparkController {
   @Autowired
    private SparkSession sparkSession;
    
    @RequestMapping("/")
    public  String home () {
    
    return "home";
    }
    @RequestMapping("/main")
    public  String index () {
    
    return "index";
}
    @RequestMapping("/pie")
    public  String pie () {
    
    return "pie";
}
    @RequestMapping("/titlebar")
    public  String titleBar () {
    
    return "titlebar";
}
    @RequestMapping("/areabar")
    public  String areaBar () {
    
    return "areabar";
}
    @RequestMapping("/read-csv")
    public ResponseEntity<String> readAndViewData() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("Wuzzuf_Jobs.csv");
        WazzafServices read = new WazzafServices();
        String html=read.readAndView(dataset);
        
        return ResponseEntity.ok(html);
    }
    @RequestMapping("/summary")
    public ResponseEntity<String> displaySchemaAndSummary() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("Wuzzuf_Jobs.csv");
        WazzafServices summary = new WazzafServices();
        String html=summary.schemaAndSummary(dataset);
        
        return ResponseEntity.ok(html);
    }
    @RequestMapping("/clean")
    public ResponseEntity<String> clenData() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("Wuzzuf_Jobs.csv");
        WazzafServices cleanSummary = new WazzafServices();
        String html=cleanSummary.clean(dataset);
        
        return ResponseEntity.ok(html);
    }
    @RequestMapping("/jops-for-company")
    public ResponseEntity<String> jopsForEachCompany() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("Wuzzuf_Jobs.csv");
        WazzafServices jopsNumEachCompany = new WazzafServices();
        String html = jopsNumEachCompany.jopsPerCompany(dataset);
        
        return ResponseEntity.ok(html);
    }
    @RequestMapping("/jops-for-company-chart")
    public ResponseEntity<String> jopsForEachCompanyChart() throws IOException {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("Wuzzuf_Jobs.csv");
        WazzafServices jopComp = new WazzafServices();
        String html = jopComp.jopsPerCompChart(dataset);
        
        return ResponseEntity.ok(html);
    }
    @RequestMapping("/most-popular-jop")
    public ResponseEntity<String> mostPopularJop() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("Wuzzuf_Jobs.csv");
        WazzafServices jopCount = new WazzafServices();
        String html = jopCount.popularTitle(dataset);
        
        return ResponseEntity.ok(html);
    }
    @RequestMapping("/most-popular-jop-chart")
    public ResponseEntity<String> mostPopularJopChart() throws IOException {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("Wuzzuf_Jobs.csv");
        WazzafServices titleCount = new WazzafServices();
        String html = titleCount.titleChart(dataset);
        
        return ResponseEntity.ok(html);
    }
    @RequestMapping("/most-popular-area")
    public ResponseEntity<String> mostPopularArea() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("Wuzzuf_Jobs.csv");
        WazzafServices cityCount = new WazzafServices();
        String html = cityCount.popularCities(dataset);
        
        return ResponseEntity.ok(html);
    }
    @RequestMapping("/most-popular-area-chart")
    public ResponseEntity<String> mostPopularAreaChart() throws IOException {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("Wuzzuf_Jobs.csv");
        WazzafServices areaCount = new WazzafServices();
        String html = areaCount.areaChart(dataset);
       
        return ResponseEntity.ok(html);
    }
    @RequestMapping("/most-important-skills")
    public ResponseEntity<String> mostImportantSkills() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("Wuzzuf_Jobs.csv");
         WazzafServices skill = new WazzafServices();
         String html=skill.neededSkills(dataset);
       
        return ResponseEntity.ok(html);
    }
}
