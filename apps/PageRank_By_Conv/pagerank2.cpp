#include <graphlab.hpp>
#include <string>

typedef graphlab::distributed_graph<graphlab::empty, double> graph_type;

bool line_parser(graph_type& graph, const std::string& filename, const std::string& textline) {
  std::stringstream strm(textline);
  graphlab::vertex_id_type vid;
  // first entry in the line is a vertex ID
  strm >> vid;
  // insert this vertex with its label
  graph.add_vertex(vid);
  // while there are elements in the line, continue to read until we fail
  double edge_val=1.0;
  while(1){
    graphlab::vertex_id_type other_vid;
    strm >> other_vid;
    strm >> edge_val;
    if (strm.fail())
      break;
    graph.add_edge(vid, other_vid,edge_val);
  }

  return true;
}

int main(int argc, char** argv) {
	graphlab::mpi_tools::init(argc, argv);
	graphlab::distributed_control dc;

	graph_type graph(dc);
	graph.load("/Users/Crazyconv/Desktop/fyp/amazon-mst-adj.txt", line_parser);
  dc.cout() << "#vertices: " << graph.num_vertices() << " #edges:" << graph.num_edges() << std::endl;
	graph.finalize();
	// dc.cout() << "#vertices: " << graph.num_vertices() << " #edges:" << graph.num_edges() << std::endl;
	
	graphlab::mpi_tools::finalize();
}