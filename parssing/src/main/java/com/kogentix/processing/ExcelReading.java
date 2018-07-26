package com.kogentix.processing;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;


public class ExcelReading {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			System.out.println("Hello");
		FileInputStream file = new FileInputStream(new File("C:/Users/KOGENTIX/Desktop/Goles/TestGsk.xls"));
		System.out.println("file readed");
		HSSFWorkbook workbook = new HSSFWorkbook(file);
		//Get first sheet from the workbook
		HSSFSheet sheet = workbook.getSheetAt(0);
		//Iterate through each rows from first sheet
		Iterator<Row> rowIterator = sheet.iterator();
		while(rowIterator.hasNext()) {
			Row row = rowIterator.next();
			
			//For each row, iterate through each columns
			Iterator<Cell> cellIterator = row.cellIterator();
			while(cellIterator.hasNext()) {
				
				Cell cell = cellIterator.next();
			}
			System.out.println("");
		}	
		file.close();
		FileOutputStream out = 
			new FileOutputStream(new File("C:/Users/KOGENTIX/Desktop/Test_Aparna.csv"));
		workbook.write(out);
		out.close();
		
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		
}}
}
