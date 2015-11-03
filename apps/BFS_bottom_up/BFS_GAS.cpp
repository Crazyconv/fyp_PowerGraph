#include <graphlab.hpp>
#include <string>
#include <stdlib.h>

int source = -1;

struct vertex_data : graphlab::IS_POD_TYPE {
	int level;
	int parent;
	vertex_data(int level = -1, int parent = -1) : level(level), parent(parent) {}
};

struct gather_data : graphlab::IS_POD_TYPE {
	int level;
	int parent;
	gather_data(int level = -1, int parent = -1): level(level), parent(parent) {}

	gather_data & operator+=(const gather_data & other){
		if(level == -1){
			level = other.level;
			parent = other.parent;
		}
		return *this;
	}
};

typedef graphlab::empty edge_data;
typedef graphlab::distributed_graph<vertex_data, edge_data> graph_type;

void init_vertex(graph_type::vertex_type& vertex) {
	if(vertex.id() == source){
		vertex.data().level = 0;
		vertex.data().parent = source;
	}
}

class BFS: public graphlab::ivertex_program<graph_type, gather_data>, public graphlab::IS_POD_TYPE{
public:
	edge_dir_type gather_edges(icontext_type& context, const vertex_type& vertex) const{
		if(vertex.data().level >= 0){
			return graphlab::NO_EDGES;
		}
		else
			return graphlab::IN_EDGES;
	}

	gather_data gather(icontext_type& context, const vertex_type& vertex, edge_type& edge) const{
		return gather_data(edge.source().data().level, edge.source().id());
	}

	void apply(icontext_type& context, vertex_type& vertex, const gather_type& total){
		if(vertex.data().level < 0){
			if(((gather_data)total).level < 0)
				context.signal(vertex);
			else{
				vertex.data().level = total.level + 1;
				vertex.data().parent = total.parent;
			}
		}
	}

	edge_dir_type scatter_edges(icontext_type& context, const vertex_type& vertex) const{
		return graphlab::NO_EDGES;
	}
	void scatter(icontext_type& context, const vertex_type& vertex, edge_type& edge) const{ }
};

bool line_parser(graph_type& graph, const std::string& filename, const std::string& textline){
	std::stringstream strm(textline);
	graphlab::vertex_id_type vid;

	strm >> vid;
	graph.add_vertex(vid, vertex_data());
	while(1){
		graphlab::vertex_id_type other_vid;
		strm >> other_vid;
		if(strm.fail())
			return true;
		graph.add_edge(vid, other_vid);
	}
	return true;
}

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

	graph.transform_vertices(init_vertex);
	// Running The Engine -------------------------------------------------------
	graphlab::omni_engine<BFS> engine(dc, graph, "sync");

	engine.signal_all();
	engine.start();

	dc.cout() << engine.num_updates()
	        << " updates." << std::endl;


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