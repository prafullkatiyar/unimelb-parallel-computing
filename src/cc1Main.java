// INCLUDE libraries for JSON Jackson / File Reading / MPI
import org.codehaus.jackson.map.*;
import org.codehaus.jackson.*;
import java.io.FileReader;


import mpi.MPI;

public class cc1Main {
	 @SuppressWarnings("null")
	public static void main(String[] args) throws Exception {
		double tic = 0;
		//----------------------------------------------------------------------------------------------- 
		// Variable declarations
		double gxMin, gxMax, gyMin, gyMax; 					// changing grid Coordinates in gridArray from String To Double
		double yCord; 								     	// for Coordinate values from InstagramFile
		double xCord;
		int temp; 											// Counter for Grids user count
		String cord;
		//-----------------------------------------------------------------------------------------------
		
		//-----------------------------------------------------------------------------------------------
		// MPI Configurations
		MPI.Init(args);
		int world_rank = MPI.COMM_WORLD.Rank();
		int world_size = MPI.COMM_WORLD.Size();
		
		//-----------------------------------------------------------------------------------------------
		
		
		//----------------------------------------------------------------------------------------------
		// Fetching values from melbGrid file into gridArray 
		// Note: This will work till melbGrid Size is small
		ObjectMapper mapper = new ObjectMapper();
	  	JsonNode gridRootArray = mapper.readTree(new FileReader("C:\\\\Users\\\\Prafull\\\\eclipse-workspace\\\\testFiles\\\\melbGrid.json"));
		Grid objGrid = new Grid();
		String[][] gridArray = objGrid.getGrid(gridRootArray);
		int gridLength = gridArray.length;
		//-----------------------------------------------------------------------------------------------
		
		
		//-----------------------------------------------------------------------------------------------
		// Getting object for Instagram file to access line by line 
		// Note# File name can also be passed as command line argument createJsonParser(new File(args[0]))
		JsonFactory f = new MappingJsonFactory();
		JsonParser jp = f.createJsonParser(new FileReader("C:\\\\Users\\\\Prafull\\\\eclipse-workspace\\\\testFiles\\\\tinyInstagram.json"));
		JsonToken current;
		current = jp.nextToken();
		if (current != JsonToken.START_OBJECT) 
		{
			System.out.println("Error: root should be object: quiting.");
			return;
		}
		
		//-----------------------------------------------------------------------------------------------
		
		
		//-----------------------------------------------------------------------------------------------
		// First while loop is executed on 3 objects, out of which we need to process ROW
		while (jp.nextToken() != JsonToken.END_OBJECT)
		{
			String fieldName = jp.getCurrentName();      		// Fetching field names
			current = jp.nextToken();                    		// Moving from field name to field value
			if (fieldName.equals("rows"))
      		{
				if (current == JsonToken.START_ARRAY) 
      			{
					tic = MPI.Wtime();
					// Processing each record in ROW array
					while (jp.nextToken() != JsonToken.END_ARRAY) 
      				{
						JsonNode node = jp.readValueAsTree(); 	 // read the record into a tree model,  this moves the parsing position to the end of it
						JsonNode doc = node.get("doc");          // with tree model we can access random data
						if (doc.isMissingNode()) { /* System.out.println("Parent Node : Doc not found "); */ } 
						else 
      					{
							JsonNode childField = doc.get("coordinates");
							if (!doc.has("coordinates")) { 	/* System.out.println("Node Coordinates not found "); */  }
							else 
      						{
								JsonNode array = childField.get("coordinates");
								if (array.isMissingNode()) 	{ /* System.out.println("Co ordinates array missing "); */ }
								else 
      							{
									JsonNode jsonNodeY = array.get(0); // this is actually Y
      								JsonNode jsonNodeX = array.get(1); // this is actually X
      								yCord = jsonNodeY.asDouble();
      								xCord = jsonNodeX.asDouble();
      								
      								
      								/////////// parallel execution ///////////////////
      								if(world_size > 1)
      								{
	      								if(world_rank%2 == 0) // world rank even
	      								{
	      									//System.out.println(world_rank);
	      									double rX = xCord;      // === to xCord ==== received X coordinate
											double rY = yCord;      // === to xCord ==== received X coordinate
											
											for(int k=0;k<gridLength;k++)
			      							{
												gxMax = Double.parseDouble(gridArray[k][2]);
		      									gxMin = Double.parseDouble(gridArray[k][1]);
		      									gyMax = Double.parseDouble(gridArray[k][4]);
		      									gyMin = Double.parseDouble(gridArray[k][3]);
		      									
		      									if( (rX < gxMax && gxMin < rX && gyMin < rY && gyMax > rY ) || (rX == gxMin && rY < gyMax && rY >= gyMin
			      									    && rY == gyMin && rX < gxMax && rX >= gxMin) || ( rY == -37.500000 && rX > gxMin && rX < gxMax) || (rX == 145.300000 && gyMin > -37.800000 && rY > gyMin && rY < gyMax
			      									    && rX==145.450000 && rY < gyMax) || ( rY > gyMin && rX==145.450000 && rY < gyMax && rY > gyMin))
			     								{
		      										
		      										char[] message = new char[2];
			      									message = gridArray[k][0].toCharArray();
		      										//System.out.println("3hody");
		      										if(world_size%2!=0) // world size odd
			      									{
		      											if(world_rank != world_size-1) {
			      											MPI.COMM_WORLD.Send(message,0, message.length, MPI.CHAR, world_rank+1, 99) ;
			      										}
			      									}
			      									else // world size even
			      									{
			      										MPI.COMM_WORLD.Send(message,0, message.length, MPI.CHAR, world_rank+1, 99) ;
			      									}
		      										
		      										
		      										//temp = (Integer.valueOf(gridArray[k][5])+1);
		      										//gridArray[k][5] = Integer.toString(temp);
			     								}
		      									
		      									
	
		      									 
		      									
			      							}
	      									
	      									

	      									
	      									
	      									//System.out.println("Rank 1st---"+world_rank);
	      									
	      								}
	      								else  // world rank odd
	      								{
	      									char[] message = new char[2];
											MPI.COMM_WORLD.Recv(message, 0, message.length, MPI.CHAR, MPI.ANY_SOURCE, 99);
											
//											System.out.println("[["+message[0]+message[1]+"]]");
											//System.out.println(world_rank);
											cord = ""+message[0]+message[1];
											for(int k=0;k<gridLength;k++)
			      							{
												if(gridArray[k][0].equals((cord)))
												{
													System.out.println(gridArray[k][0]);
													temp = (Integer.valueOf(gridArray[k][5])+1);
													System.out.println(temp);
		      										gridArray[k][5] = Integer.toString(temp);
		      										System.out.println(gridArray[k][5]);
												}
			      							}
											
											
											//double rX = message[0];      // === to xCord ==== received X coordinate
											//double rY = message[1];      // === to xCord ==== received X coordinate
											
											 
											
											
	      								}
      								}
      								if(world_size==1)
      								{
      									double rX = xCord;      // === to xCord ==== received X coordinate
										double rY = yCord;      // === to xCord ==== received X coordinate
										
										for(int k=0;k<gridLength;k++)
		      							{
											gxMax = Double.parseDouble(gridArray[k][2]);
	      									gxMin = Double.parseDouble(gridArray[k][1]);
	      									gyMax = Double.parseDouble(gridArray[k][4]);
	      									gyMin = Double.parseDouble(gridArray[k][3]);
	      									
	      									if( rX < gxMax && gxMin < rX && gyMin < rY && gyMax > rY)
		     								{
	      										temp = (Integer.valueOf(gridArray[k][5])+1);
	      										gridArray[k][5] = Integer.toString(temp);
		     								}
	      									
	      									

	      									if(rX == gxMin && rY < gyMax && rY >= gyMin
		      									    && rY == gyMin && rX < gxMax && rX >= gxMin
		      										)
	      									{
		      										 
	      										temp = (Integer.valueOf(gridArray[k][5])+1);
	      										gridArray[k][5] = Integer.toString(temp); 
	      									}
	      									
	      									
	      									// --- for A1 --- A4 top border line
	      									if(rY == -37.500000 && rX > gxMin && rX < gxMax ) {
	      										temp = (Integer.valueOf(gridArray[k][5])+1);
	      										gridArray[k][5] = Integer.toString(temp);
	      									}
	      									
	      									// for right border of A4 B4
	      									if(rX == 145.300000 && gyMin > -37.800000 && rY > gyMin && rY < gyMax)
	      									{
	      										temp = (Integer.valueOf(gridArray[k][5])+1);
	      										gridArray[k][5] = Integer.toString(temp);
	      									}
	      									
	      									// Right border of D5 C5
	      									if(rX==145.450000 && rY < gyMax && rY > gyMin) {
	      										temp = (Integer.valueOf(gridArray[k][5])+1);
	      										gridArray[k][5] = Integer.toString(temp);
	      									}
	      									
	      									// C5 top border
	      									if(rX==145.450000 && rY < gyMax && rY > gyMin) {
	      										temp = (Integer.valueOf(gridArray[k][5])+1);
	      										gridArray[k][5] = Integer.toString(temp);
	      									}
	      									
		      							}
      								}
      								/////////// parallel execution ///////////////////
      							}
      							
      						}
      					}
						
						
						
						
      				}
      			}
      		}
			double toc = MPI.Wtime();
	        System.out.println("Time taken is " + (toc-tic)+"seconds");
		}
		
		
		if(world_rank == 0)
		{
			System.out.println("sdfsdbjh");
			System.out.println(gridArray[9][5]);
			System.out.println(gridArray[11][5]);
			System.out.println(gridArray[10][5]);
			System.out.println(gridArray[0][3]);
			System.out.println(gridArray[0][4]);
			System.out.println(gridArray[0][5]);
			// --------------- OUTPUT 1 ; All grids sorted ----------------------
			String[][] output1 = gridArray;
			
			java.util.Arrays.sort(output1, java.util.Comparator.comparingDouble(a -> Double.parseDouble(a[5])));
			for(int d=gridLength-1;d>=0;d--)
			{
				System.out.println("Grid : -  "+output1[d][0]+" | Count : - "+output1[d][5]);
			}
			// --------------- OUTPUT 1 ; All grids sorted ----------------------
			
			
			// --------------- OUTPUT 2 ;; starts here -----------------------------------
			String[][] output2= new String[4][2];
			output2[0][1] = output2[1][1] = output2[2][1] = output2[3][1] = "0";
			for(int d1=0;d1 < gridLength; d1++)
			{
				if(gridArray[d1][0].substring(0,1).equals("A"))
				{
					output2[0][0] = "A";
					
					temp = (Integer.valueOf(gridArray[d1][5]) + Integer.valueOf(output2[0][1]));
					output2[0][1] = Integer.toString(temp);
					
				}
				if(gridArray[d1][0].substring(0,1).equals("B"))
				{
					output2[1][0] = "B";
					temp = (Integer.valueOf(gridArray[d1][5]) + Integer.valueOf(output2[1][1]));
					output2[1][1] = Integer.toString(temp);
				}
				if(gridArray[d1][0].substring(0,1).equals("C"))
				{
					output2[2][0] = "C";
					temp = (Integer.valueOf(gridArray[d1][5]) + Integer.valueOf(output2[2][1]));
					output2[2][1] = Integer.toString(temp);
				}
				if(gridArray[d1][0].substring(0,1).equals("D"))
				{
					output2[3][0] = "D";
					temp = (Integer.valueOf(gridArray[d1][5]) + Integer.valueOf(output2[3][1]));
					output2[3][1] = Integer.toString(temp);
				}
				
			}
			java.util.Arrays.sort(output2, java.util.Comparator.comparingDouble(a -> Double.parseDouble(a[1])));
			for(int d2=3;d2>=0;d2--)
			{
				System.out.println("Grid - "+output2[d2][0]+" | Count - "+output2[d2][1]);
			}
			
			// --------------- OUTPUT 2 ; Row wise sorting ----------------------
			
			// --------------- OUTPUT 3 ; Row wise sorting ----------------------
			/*
			String[] col1 = {"A1","B1","C1"};
			String[] col2 = {"A2","B2","C2"};
			String[] col3 = {"A3","B3","C3","D3"};
			String[] col4 = {"A4","B4","C4","D4"};
			String[] col5 = {"C5","D5"};
			*/
			String[][] output3= new String[5][2];
			output3[0][1] = output3[1][1] = output3[2][1] = output3[3][1] = "0";
			for(int d1=0;d1 < gridLength; d1++)
			{
				if(gridArray[d1][0].equals("A1") || gridArray[d1][0].equals("C1") || gridArray[d1][0].equals("B1"))
				{
					output3[0][0] = "Column 1";
					
					temp = (Integer.valueOf(gridArray[d1][5]) + Integer.valueOf(output3[0][1]));
					output3[0][1] = Integer.toString(temp);
					
				}
				if(gridArray[d1][0].equals("A2") || gridArray[d1][0].equals("C2") || gridArray[d1][0].equals("B2"))
				{
					output3[1][0] = "Column 2";
					temp = (Integer.valueOf(gridArray[d1][5]) + Integer.valueOf(output3[1][1]));
					output3[1][1] = Integer.toString(temp);
				}
				if(gridArray[d1][0].equals("A3") || gridArray[d1][0].equals("C3") || gridArray[d1][0].equals("B3")  || gridArray[d1][0].equals("D3"))
				{
					output3[2][0] = "Column 3";
					temp = (Integer.valueOf(gridArray[d1][5]) + Integer.valueOf(output3[2][1]));
					output3[2][1] = Integer.toString(temp);
				}
				if(gridArray[d1][0].equals("A4") || gridArray[d1][0].equals("C4") || gridArray[d1][0].equals("B4")  || gridArray[d1][0].equals("D4"))
				{
					output3[3][0] = "Column 4";
					temp = (Integer.valueOf(gridArray[d1][5]) + Integer.valueOf(output3[3][1]));
					output3[3][1] = Integer.toString(temp);
				}
				if(gridArray[d1][0].equals("C5") || gridArray[d1][0].equals("D5"))
				{
					output3[4][0] = "Column 5";
					temp = (Integer.valueOf(gridArray[d1][5]) + Integer.valueOf(output3[3][1]));
					output3[4][1] = Integer.toString(temp);
				}
				
			}
			java.util.Arrays.sort(output2, java.util.Comparator.comparingDouble(a -> Double.parseDouble(a[1])));
			for(int d2=4;d2>=0;d2--)
			{
				System.out.println("Grid - "+output3[d2][0]+" | Count - "+output3[d2][1]);
			}
			
			
			
			
			
			
			
			// --------------- OUTPUT 3 ; Row wise sorting ----------------------
			
			
		}
		
		MPI.Finalize();
		MPI.COMM_WORLD.Barrier();
		
		
	 } // main ends here

}

//http://mpitutorial.com/tutorials/mpi-send-and-receive/
//http://mpitutorial.com/tutorials/mpi-introduction/
//http://mpitutorial.com/tutorials/mpi-introduction/