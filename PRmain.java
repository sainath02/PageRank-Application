package PRFinal;

public class PRmain {
	public static void main(String args[]) {
		if (args.length != 3) {
			System.err.println("Please input 3 relevant arguments");
		}
		try {
			//calculating graph properties
			
			GraphProperties PRGraphProperties = new GraphProperties();
			PRGraphProperties.getGraphProperties(args[0], args[1] + "GraphProperties");
			
			// Modifying input file by assigning page ranks to all nodes. 
			
			InitialPageRank inputModifier = new InitialPageRank();
			inputModifier.modifyInput(args[0], args[1] + "ModifiedInput");
			
			//Calculating the page rank iteratively until the value converges
			
			RankComputer rankComputer = new RankComputer();
			int convergenceCount = 0, prev = 0;
			long nonConvergentNodes = 0, timeFor10Iterations = 0, startTime = System.currentTimeMillis(), endTime = 0;
		
			//When all the nodes converges, we stop the loop.
			
			while (true) {
				convergenceCount++;
				if (convergenceCount == 1) {
					nonConvergentNodes = rankComputer.ComputeRank(args[1] + "ModifiedInput", args[1] + "TempOutput/"+ convergenceCount, args[2]);
					prev = convergenceCount;
				} else {
					nonConvergentNodes = rankComputer.ComputeRank(args[1] + "TempOutput/" + prev,args[1] + "TempOutput/" + convergenceCount,args[2]);
					prev = convergenceCount;
				}
				if (nonConvergentNodes == 0) {
					break;
				}
				if (convergenceCount == 10) {
					timeFor10Iterations = System.currentTimeMillis();
				}
			}
			
			//After convergence occurred on all nodes, write the PageRank values of all nodes in output file 
			
			PageRankOutput pageRankOutput = new PageRankOutput();
			pageRankOutput.OutputPageRank(args[1] + "TempOutput/" + convergenceCount, args[1] + "Output");

			endTime = System.currentTimeMillis();

			System.out.println(args[1] + "GraphProperties" + " contains details of the Graph");
			System.out.println(args[1] + "Output" + " contains Page Rank values of each node");
			System.out.println("No Of Iterations for convergance is "+ convergenceCount);
			System.out.println("Time taken for first 10 iterations is "+ (timeFor10Iterations - startTime) + " MilliSeconds");
			System.out.println("Take taken for total Execution is "	+ (endTime - startTime) + " MilliSeconds");

		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
