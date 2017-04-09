package org.myorg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 * Jay Dipak Shah
 * PID 800326050
 */

public class PageRank extends Configured implements Tool {

 
   
   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new PageRank(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
     // set configuration
      Configuration conf1 = new Configuration();
    // set the directories, output and intermediate folders
      String int_dir = "intermediateDirectory";
      
      Path outputPath = new Path(args[1]);
      Path intermediatePath = new Path(int_dir);
      FileSystem hdfs = FileSystem.get(conf1);
      // if my intermediate directory exists and output also exists then go and delete it
      // you dont have to worry about cleaning up intermediate everytime you run. 
      try {
		   if(hdfs.exists(outputPath)){
			   hdfs.delete(outputPath, true);
		   } if(hdfs.exists(intermediatePath)){
			   hdfs.delete(intermediatePath, true);
		   }
			hdfs.mkdirs(intermediatePath);
				
		} catch (IOException e) {
				e.printStackTrace();
		}
    
      
      // lets create a path for the couting of the pages
      Path num_pages = new Path(intermediatePath, "countThePages");
      
      
      // get the instance of job 1
      Job job1  = Job .getInstance(conf1, " CountThePagesJob1 ");
      job1.setJarByClass( this .getClass());

      //read in the file path for input for job 1, which is read from the command line
      FileInputFormat.addInputPaths(job1,  args[0]);
      //the output path of job1 will be the num_pages path I created above, which will be in the intermediate directory
      FileOutputFormat.setOutputPath(job1,  num_pages);
      
      // set the mapper and reducer classes and their output keys and values types
      job1.setMapperClass( MapCountTheNodes .class);
      job1.setReducerClass( ReduceCountTheNodes .class);
      job1.setOutputKeyClass( Text .class);
      job1.setOutputValueClass( IntWritable .class);
      
      //is the first job done?, of so assign 0 other wise 1
      int Is_Job_One_Done = job1.waitForCompletion(true) ? 0:1;
      
      System.out.println("THE COUNTING OF THE NODES IN THE GRAPH ARE FINISHED, JOB 1 DONE");
      // if job one is done we proceed to job 2, this is where we will give initial page rank values
      int Is_Job_Two_Done = 0;
      int Is_Job_Three_Done = 0;
      int Is_Job_Four_Done =0;
      
      
      if(Is_Job_One_Done ==  0){
    	  // configure the second job
    	  Configuration config2 = new Configuration();  
    	  String count_line=""; 
    	 
        	 
        	// for this job we need initial page ranks, the nubmer of nodes is stored in the intermediate
        	  // file called countnumpages, we need to grab this file and get the value of the nodes
    		  FileSystem job2FileSystem = FileSystem.get(config2);
        	  Path get_nodeCount_file = new Path(num_pages,"part-r-00000");
        	  
        	 
              
              
              // open the file and get the buffered reader ojbect to read in the file line by line
              
              BufferedReader buff_read =new BufferedReader(new InputStreamReader(job2FileSystem.open(get_nodeCount_file)));
               
              // if you indeed read in a line, then save it to the count_line; 
              int total_nodes_initial_N = 1;
              while((count_line=buff_read.readLine()) != null){
            	       if(count_line.trim().length()>0){
            	    	   if(count_line.length() > 1){
            	        		  // split on the space 
            	    			  String [] s = count_line.split("\\s+");
            	        		  total_nodes_initial_N = Integer.parseInt(s[1]);
            	       }
                     
                      
              }
              }
              buff_read.close();
              
          // now that we have the integer that we want, we need to make the job 2 instance
    	  
    	  Job job2_LinkGraph = Job.getInstance(config2,"GetTheLinkGraph");
    	  // set the jar file for this class to be pagerank's jar
    	  job2_LinkGraph.setJarByClass(PageRank.class);
    	  
    	  // we need to get the graph file
    	  FileInputFormat.addInputPaths(job2_LinkGraph,args[0]);
    	  
    	  // create the output path for the link graph that we get: 
    	 Path path_to_link_graph_job2 = new Path(intermediatePath,"job2_linkGraph_result");
    	  
    	  // now that we have a path we can create an output path:
    	  
    	  FileOutputFormat.setOutputPath(job2_LinkGraph, path_to_link_graph_job2);
    	  
    	  job2_LinkGraph.setMapperClass( MapBuildLinkGraph .class);
    	  job2_LinkGraph.setReducerClass( ReduceBuildLinkGraph .class);
    	  job2_LinkGraph.setMapOutputKeyClass(Text.class);
    	  job2_LinkGraph.setMapOutputValueClass(Text.class);
    	  job2_LinkGraph.setOutputKeyClass( Text .class);
    	  job2_LinkGraph.setOutputValueClass( Text .class);
    	  
    	  // we need to make the total nodes available inside job2 to use for initial page rank
    	  // the variable is called "Node_ Number"
    	  job2_LinkGraph.getConfiguration().setInt("Node_Number", total_nodes_initial_N);
    	  
    	  Is_Job_Two_Done =  job2_LinkGraph.waitForCompletion(true)? 0 : 1;

   
     
      if(Is_Job_Two_Done==0){
    	  
    	  // as per guidelines, we need to iterate this 10 times so page ranks converges
    	  for(int i = 0;i<10;i++){
    		// if Job 3 is done, then set the new configuration for map/reduce 3
        	  
        	  Configuration config3 = new Configuration();
        	  
        	  // set the job instance for job3
        	  Job job3_PageRank = Job.getInstance(config3,"Page_Rank_Calculation");
        	  job3_PageRank.setJarByClass(PageRank.class);
        	  
        	  // we set the path to store the intermediate files because this is iterative
        	  Path intermediate_file_path = new Path(intermediatePath, "iteration"+i);  
        	  // add the usual input andoutput paths
        	  // the input to job 3 will be the link graph that we generated in the last job
        	  FileInputFormat.addInputPath(job3_PageRank, path_to_link_graph_job2 );
        	  // set the output path
        	  FileOutputFormat.setOutputPath(job3_PageRank, intermediate_file_path);
        	  //set the mapper class
        	  job3_PageRank.setMapperClass(Map_for_PageRank.class);
        	  job3_PageRank.setReducerClass(Reduce_for_PageRank.class);
        	  
        	  job3_PageRank.setInputFormatClass(KeyValueTextInputFormat.class);
        	  job3_PageRank.setOutputFormatClass(TextOutputFormat.class);
        	  // this sets the values types for the ouptut for both map and reduce in job3
        	  job3_PageRank.setOutputKeyClass(Text.class);
        	  job3_PageRank.setOutputValueClass(Text.class);
        	  
        	  Is_Job_Three_Done = job3_PageRank.waitForCompletion(true) ? 0:1;
        	  
        	  path_to_link_graph_job2 = intermediate_file_path;
    	  }
  
      }// end if job 3 if statement
      
      if(Is_Job_Three_Done == 0){
    	  // COnfigure the last job to sort and clean up 
    	  Configuration config4 = new Configuration();
    	  // make the job instance
    	  Job job4_Sort = Job.getInstance(config4, "Page_Rank_Sort");
    	  job4_Sort.setJarByClass(PageRank.class);
    	  // we are gonna get the last iteration of last job, this is the input to the sorting 
    	  // it has the most updated page ranks
    	  FileInputFormat.addInputPath(job4_Sort,path_to_link_graph_job2);
    	  FileOutputFormat.setOutputPath(job4_Sort,new Path(args[1]));
    	  // per guideline in assignment, we only use 1 reducer for the sorting
    	  job4_Sort.setNumReduceTasks(1);
    	  //set the input format class. 
    	  job4_Sort.setInputFormatClass(KeyValueTextInputFormat.class);
    	  job4_Sort.setOutputFormatClass(TextOutputFormat.class);
    	  // this sets the mapper and reducer classes
    	  job4_Sort.setMapperClass(Map_PageRank_Sort.class);
    	  job4_Sort.setReducerClass(Reduce_PageRank_Sort.class);
    	  
    	// additionally we need to set a new comparator so make a new class to do that
    	  // we override it, because we need to do decsending order
    	  job4_Sort.setSortComparatorClass(ReverseSortClass.class);
    	  
    	  job4_Sort.setMapOutputKeyClass(DoubleWritable.class);
    	  job4_Sort.setMapOutputValueClass(Text.class);
    	  
    	 job4_Sort.setOutputKeyClass(Text.class);
    	 job4_Sort.setOutputKeyClass(DoubleWritable.class);
    	  
    	 
    	  
    	  //hdfs.delete(intermediatePath,true);
    	  Is_Job_Four_Done = job4_Sort.waitForCompletion(true) ? 0 : 1;
    	  
      }
      }// end of the if statement for job 2
      return Is_Job_Four_Done;
   }// END OF RUN METHOD
/* -----------------------------------------------------------------------------------------------------------------*/
   public static class MapCountTheNodes extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
     private final static IntWritable one = new IntWritable(1);
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
         // if the line text coming in exists then we need to assign 1 to each line
    	 // this map job will assign a value of 1 for each page in order to count the number of nodes
    	  if(lineText != null){
    		  if(lineText.toString().trim().length()>0){
    			  
    			  context.write(new Text("line"),one);  
    		  } // end of inner if
    	  } // end of outer if
            
            
         } // end of map function
      
      }// end of MapCount the Nodes Class
   

   public static class ReduceCountTheNodes extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int sum  = 0;
         // add up all the ones, this gives us the number of nodes
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }
         context.write(new Text("Number_of_Nodes"),  new IntWritable(sum));
         
      }
   }
 

/* -----------------------------------------------------------------------------------------------------------------*/
public static class MapBuildLinkGraph extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
    private final static IntWritable one  = new IntWritable( 1);
    private Text word  = new Text();

    public void map( LongWritable offset,  Text lineText,  Context context)
      throws  IOException,  InterruptedException {
      
    	// convert line to string so we can process and grab what we want
    	String string_line  = lineText.toString();
        
    	// all the titles of the nodes are between text tags so we need to get them
    	Pattern get_titles = Pattern.compile("<title>(.+?)</title>");
    
    	// the actual outlinks are between the double brackets
    	Pattern  get_each_outlink = Pattern.compile("\\[\\[.*?]\\]");
    	
	
    	if(string_line != null && string_line.trim().length()>0){
    		try{
    			// NOTE TO SELF: we have to use try catch because I get a Illegal start of exception ... 
    			
    		

    			// use the matcher to find the title pattern in the line text
    			Matcher match_title_pattern = get_titles.matcher(string_line);
    			match_title_pattern.find();
    		
    			//get the originl nodes
    			String original_nodes = match_title_pattern.group(1);
    			// we now have the <title> ---</title> part of the text
    			// we now need the links that each title is associated with
    			if(!original_nodes.isEmpty() && original_nodes !=null){
    				
    			
    			// we need to match the outlinks, thus we need to find that regex pattern.
    			Matcher out_going_links_matcher = get_each_outlink.matcher(string_line);
    			// we need to iterate over the links and output the original node with the urls it is associate with
    		
    			while(out_going_links_matcher.find()){
    				String out_links = out_going_links_matcher.group().replace("[[", "");
    				out_links = out_links.replace("]]", "");
    				// puts out the original node and a correspnding link in the list
    				if(!out_links.isEmpty()){
    					context.write(new Text(original_nodes), new Text(out_links));
    				}
    				
    			}
    			}
    	} catch(Exception e){
    		System.out.println("Something went wrong in your building link graph");
    	}
    	}
       } // end of map function
    }// end of MapCount the Nodes Class
 

 public static class ReduceBuildLinkGraph extends Reducer<Text ,  Text ,  Text ,  Text > {
    @Override 
    public void reduce( Text originalNode,  Iterable<Text > counts,  Context context)
       throws IOException,  InterruptedException {
    	// get the value of the number of nodes/lines in the original file so we can 
    	// calculate initial page rank
        double number_nodes = (double)context.getConfiguration().getInt("Node_Number",1);
        //this is the initial page rank 1/N, we got n from the file saved in first job
        double pageRank = 1.0/number_nodes;
    	
        // we have the page rank initial and now we need to output that along with the urls for each
        // original node
        String rank_and_urls = pageRank +"###";
        for(Text count : counts){
        	rank_and_urls = rank_and_urls + count.toString() +">";
        }
       context.write(originalNode,  new Text(rank_and_urls));
    }
  }
 
 /* -----------------------------------------------------------------------------------------------------------------*/
 
 
 public static class Map_for_PageRank extends Mapper<Text ,  Text ,  Text ,  Text > {
   //  private final static IntWritable one  = new IntWritable( 1);
     //private Text word  = new Text();

     public void map( Text Node,  Text lineText,  Context context)
       throws  IOException,  InterruptedException {
    	 
        // put this URL and its list of nodes back out again
    	 context.write(Node,lineText);
    	 
    	 // the line text is of the format : PRinit###q1>q2... take th elineText and split on ###
    	 // get the nodes list (of the format q0>q1> etc...)
    	 String [] split_line = lineText.toString().split("###"); 
    	 
    	 
    	 // get the incoming page rank initial
    	 String page_rank_init = split_line[0];
    	 // convert it to a double
    	 double page_rank = Double.parseDouble(page_rank_init);
    	 // if there is something that exists in the line,
    	 if(split_line.length>1){
    		 // then get the nodes list, which is at index 1
    		 String nodes_list = split_line[1];
    		 // if there are nodes
    		 if(!nodes_list.isEmpty()){
    			 
    			 // then split the nodes list on > which separate the links
    			 String [] nodes_string_list = nodes_list.split(">");
        		 // calculate new page rank adn send it out
            	 double new_page_rank = page_rank/(double)nodes_string_list.length;
            	 String  new_rank = Double.toString(new_page_rank);
            	 for(int i =0;i < nodes_string_list.length;i++){
            		  
            		  context.write(new Text(nodes_string_list[i]), new Text(new_rank));	 
            	 }
    		 }
    		   	 
    		 
    	 }
    	 
        } // end of map function
     
     }// end of MapCount the Nodes Class
  

  public static class Reduce_for_PageRank extends Reducer<Text ,  Text ,  Text ,  Text > {
     @Override 
     public void reduce( Text Node,  Iterable<Text > ranks,  Context context)
        throws IOException,  InterruptedException {
          String urls ="";
          double new_page_rank =0.0;
          boolean bool =false;
    	 for(Text rank : ranks){
    		 
    		 String element = rank.toString();
    		 // if the element in the ranks list contains the ### pattern then thats an original node
    		 if(element.contains("###")){
    			// if it contains the hashtags, we want the URLS associated with, we dont want the old page rank
    			 String[] split = element.split("###");
    			 urls = split[1];
    			 bool = true;
    		 }else{
    			 // else, we have just a page rank and we need to add it to a sum
    			 new_page_rank += Double.parseDouble(element);
    		 }
    	 } // end of for
    	 // only if we have the original nodes we will calculate its new page ranka send it out
    	 if(bool==true){
    		 // calculate the page rank according to its formula
    		 new_page_rank = .15+new_page_rank*0.85;
    		 // makes sure its a double, even though new_page_rank already is (just to make sure)
             String newest_rank = Double.toString(new_page_rank);
        	 context.write(Node,  new Text(newest_rank+"###"+urls));
    	 }
    	 
        
     }
  }

  /* -----------------------------------------------------------------------------------------------------------------*/
  						// override the compare method that is used, we want to reverse sort the output
  
  public static class ReverseSortClass extends WritableComparator {
	   
	   protected ReverseSortClass() {
		   super(DoubleWritable.class, true);
	   }
	   
	   // this is the compare method that we will use, to override the old one
	   // this compares the two page ranks being sent in
	   public int compare ( @SuppressWarnings("rawtypes")WritableComparable key1, @SuppressWarnings("rawtypes")WritableComparable key2){
		   
		   // convert both keys to double writable
		   DoubleWritable a = (DoubleWritable) key1;
		   
		   DoubleWritable b = (DoubleWritable) key2;
		   
		  // compare method returns either 1 or -1 or 0, we just take the negative of the compare result to reverse sort it
		   
		   return -1*(a.compareTo(b));
	   }
	   
  }
  
  /* -----------------------------------------------------------------------------------------------------------------*/
  
  
  public static class Map_PageRank_Sort extends Mapper<Text ,  Text ,  DoubleWritable ,  Text > {
      // this is for sorting the output

      public void map( Text node,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
         // turn the incoming linetext to string
    	  String line = lineText.toString();
    	  // split the line on ### and get page rank at index 0
    	  String [] page_rank_array = line.split("###");
    	  double page_rank = Double.parseDouble(page_rank_array[0]);
            // note that here we are keeping page_rank as the key and node that it belongs to as the value
    	  // the sorter will sort based on keys
    	  context.write(new DoubleWritable(page_rank),node);
            
         } // end of map function
      
      }// end of MapCount the Nodes Class
   

   public static class Reduce_PageRank_Sort extends Reducer<DoubleWritable,Text ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( DoubleWritable rank,  Iterable<Text > nodes,  Context context)
         throws IOException,  InterruptedException {
         
    	              // just output the iterable with the page with the rank 
    	  			// this time the node is the key and the rank is the value, but it will be in reverse sorted order 
    	            // based on page rank 
    	  
    	  			for(Text n : nodes){
    	  				context.write(n,rank);
    	  			}
    	             
        
         
      }
   }
 
 
 
 
}// end of big class
