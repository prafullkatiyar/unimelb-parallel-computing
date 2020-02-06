import org.codehaus.jackson.JsonNode;

public class Grid {
	
	@SuppressWarnings("null")
	public String[][] getGrid(JsonNode gridRootArray) {
		
		
		
		int i = 0;
		JsonNode childRows = gridRootArray.get("features"); 
		
		int hu =  childRows.size();
		String[][] grid = new String[hu][6];
		
		int ss = childRows.size();
		//String xCord = jsonNodeRows.asText();
		for(JsonNode root : childRows)
		{
			//JsonNode jsonNodeRows = root.get(1);
    		JsonNode child = root.get("properties");
    		if (child.isMissingNode()) 
			{
				//System.out.println("Parent Node : Doc not found ");
			} 
    		else 
			{
    					
    					
    					JsonNode jsonId = child.get("id");
    					grid[i][0]  = jsonId.asText();
    					JsonNode jsonXmin = child.get("xmin");
    					grid[i][1]= jsonXmin.asText();
    					
    					JsonNode jsonXmax = child.get("xmax");
    					grid[i][2]  = jsonXmax.asText();
    					
    					JsonNode jsonYmin = child.get("ymin");
    					grid[i][3] = jsonYmin.asText();
    					
    					JsonNode jsonYmax = child.get("ymax");
    					grid[i][4] = jsonYmax.asText();
    					
    					grid[i][5] = "0";
    					
    					

		    		    // System.out.println("_____________________________________");
		    		    // System.out.println(i);
						   //if(Double.parseDouble(xCord) > 144)
		    		     //System.out.println("Id:"+jsonId.asText()+" | Xmin : "+jsonXmin.asText()+" | Xmax : "+jsonXmax.asText());
		    		     //System.out.println("Id:"+jsonId.asText()+" | Ymin : "+jsonYmin.asText()+" | Ymax : "+jsonYmax.asText());
						   //else
						   //System.out.println("out of range X");
		    		     
		    		     i++;
					
				
			}
		}
		return grid;
		
	}

}
