#include <graphlab.hpp>
#include <string>
#include <stdlib.h>

int source = -1;

struct vertex_data : graphlab::IS_POD_TYPE {
	int level;
	int parent;
	vertex_data(int level = -1, int parent = -1) : level(level), parent(parent) {}
};

struct message_data : graphlab::IS_POD_TYPE {
	int level;
	int parent;
	message_data(int level = -1, int parent = -1): level(level), parent(parent) {}

	message_data & operator+=(const message_data & other){
		return *this;
	}
};

typedef graphlab::empty edge_data;
typedef graphlab::distributed_graph<vertex_data, edge_data> graph_type;

class BFS: public graphlab::ivertex_program<graph_type, graphlab::empty, message_data>, public graphlab::IS_POD_TYPE{
private:
	bool new_frontier = false;
	message_data state;
public:
	void init(icontext_type& context, const vertex_type& vertex, const message_data& message) {
		state = message;
	} 

	edge_dir_type gather_edges(icontext_type& context, const vertex_type& vertex) const{
		return graphlab::NO_EDGES;
	}

	void apply(icontext_type& context, vertex_type& vertex, const gather_type& total){
		if(vertex.data().level < 0 && state.level >= 0){
			new_frontier = true;
			vertex.data().level = state.level;
			vertex.data().parent = state.parent;
		}
	}

	edge_dir_type scatter_edges(icontext_type& context, const vertex_type& vertex) const{
		if(new_frontier)
			return graphlab::OUT_EDGES;
		else
			return graphlab::NO_EDGES;
	}
	void scatter(icontext_type& context, const vertex_type& vertex, edge_type& edge) const{
		context.signal(edge.target(), message_data(vertex.data().level + 1, vertex.id()));
	}
};

// bool line_parser(graph_type& graph, const std::string& filename, const std::string& textline){
// 	std::stringstream strm(textline);
// 	graphlab::vertex_id_type vid;

// 	strm >> vid;
// 	graph.add_vertex(vid, vertex_data());
// 	while(1){
// 		graphlab::vertex_id_type other_vid;
// 		strm >> other_vid;
// 		if(strm.fail())
// 			return true;
// 		graph.add_edge(vid, other_vid);
// 	}
// 	return true;
// }

class graph_writer{
public:
	std::string save_vertex(graph_type::vertex_type v){
		std::stringstream strm;
		strm << v.id() << "\t" << v.data().level << "\t" << v.data().parent << "\n";
		return strm.str();
	} 
	std::string save_edge(graph_type::edge_type e){
		return "";
	}
};

int main(int argc, char** argv) {
	graphlab::mpi_tools::init(argc, argv);
	graphlab::distributed_control dc;
	
	std::string graph_dir;
	std::string saveprefix;
	std::string format;
	size_t powerlaw = 0;

	// Parse command line options -----------------------------------------------
	graphlab::command_line_options clopts("PageRank algorithm.");
	clopts.attach_option("graph", graph_dir,
		"The graph file.  If none is provided "
		"then a toy graph will be created");
	clopts.add_positional("graph");
	clopts.attach_option("powerlaw", powerlaw,
		"Generate a synthetic powerlaw out-degree graph. ");
	clopts.attach_option("saveprefix", saveprefix,
		"If set, will save the resultant pagerank to a "
		"sequence of files with prefix saveprefix");
	clopts.attach_option("source", source,
		"The source vertice");
	clopts.attach_option("format", format,
		"The graph format");

	if(!clopts.parse(argc, argv)) {
		dc.cout() << "Error in parsing command line arguments." << std::endl;
		return EXIT_FAILURE;
	}

	// Build the graph ----------------------------------------------------------
	graph_type graph(dc, clopts);
	if(powerlaw > 0) { // make a synthetic graph
		dc.cout() << "Loading synthetic Powerlaw graph." << std::endl;
		graph.load_synthetic_powerlaw(powerlaw, false, 2.1, 100000000);
	} else if (graph_dir.length() > 0) { // Load the graph from a file
		if(format.length() == 0){
			dc.cout() << "No graph format provided. Use snap format" << std::endl;
			format = "snap";
		}
		dc.cout() << "Loading graph" << std::endl;
		graph.load_format(graph_dir, format);
	} else {
		dc.cout() << "graph or powerlaw option must be specified" << std::endl;
		clopts.print_description();
		return 0;
	}
	if(source < 0) {
		dc.cout() << "No source vertex provided. Use vertex 0 as source" << std::endl;
		source = 0;
	}

	// must call finalize before querying the graph
	graph.finalize();
	dc.cout() << "#vertices: " << graph.num_vertices()
	        << " #edges:" << graph.num_edges() << std::endl;

	// Running The Engine -------------------------------------------------------
	graphlab::omni_engine<BFS> engine(dc, graph, "sync");

	engine.signal(source, message_data(0, source));
	engine.start();

	dc.cout() << engine.num_updates() << " updates." << std::endl;
	dc.cout() << "Finished in " << engine.elapsed_seconds() << " seconds." << std::endl;

	// Save the final graph -----------------------------------------------------
	if (saveprefix != "") {
	graph.save(saveprefix, graph_writer(),
	           false,    // do not gzip
	           true,     // save vertices
	           false);   // do not save edges
	}

	graphlab::mpi_tools::finalize();
	return EXIT_SUCCESS;

}