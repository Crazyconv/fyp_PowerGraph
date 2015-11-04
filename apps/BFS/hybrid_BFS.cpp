#include <graphlab.hpp>
#include <string>
#include <stdlib.h>

// global variable
// any currency problem?
int last_iter = 0;

struct vertex_data : graphlab::IS_POD_TYPE {
	int level;
	int parent;
	vertex_data(int level = -2, int parent = -1) : level(level), parent(parent) {}
};

// used in bottom_up
struct gather_data : graphlab::IS_POD_TYPE {
	int level;
	int parent;
	gather_data(int level = -2, int parent = -1): level(level), parent(parent) {}

	gather_data & operator+=(const gather_data & other){
		if(level <= 0){
			level = other.level;
			parent = other.parent;
		}
		return *this;
	}
};

// used in top_down
struct message_data : graphlab::IS_POD_TYPE {
	bool update_self;
	int level;
	int parent;
	message_data(bool update_self = true, int level = -2, int parent = -1): 
		update_self(update_self), level(level), parent(parent) {}

	message_data & operator+=(const message_data & other){
		return *this;
	}
};

typedef graphlab::empty edge_data;
typedef graphlab::distributed_graph<vertex_data, edge_data> graph_type;

class BFS_bottomup: public graphlab::ivertex_program<graph_type, gather_data, graphlab::empty>, public graphlab::IS_POD_TYPE{
public:
	edge_dir_type gather_edges(icontext_type& context, const vertex_type& vertex) const{
		if(vertex.data().level >= 0){
			return graphlab::NO_EDGES;
		}
		else
			return graphlab::IN_EDGES;
	}

	gather_data gather(icontext_type& context, const vertex_type& vertex, edge_type& edge) const{
		return gather_data(edge.source().data().level + 1, edge.source().id());
	}

	void apply(icontext_type& context, vertex_type& vertex, const gather_type& total){
		if(vertex.data().level < 0 && ((gather_data)total).level > 0){
			vertex.data().level = total.level;
			vertex.data().parent = total.parent;
		}
	}

	edge_dir_type scatter_edges(icontext_type& context, const vertex_type& vertex) const{
		return graphlab::NO_EDGES;
	}
};

class BFS_topdown: public graphlab::ivertex_program<graph_type, graphlab::empty, message_data>, public graphlab::IS_POD_TYPE{
private:
	message_data state;
public:
	void init(icontext_type& context, const vertex_type& vertex, const message_data& message) {
		state = message;
	} 

	edge_dir_type gather_edges(icontext_type& context, const vertex_type& vertex) const{
		return graphlab::NO_EDGES;
	}

	void apply(icontext_type& context, vertex_type& vertex, const gather_type& total){
		if(state.update_self){
			if(vertex.data().level < 0){
				vertex.data().level = state.level;
				vertex.data().parent = state.parent;
			}
		}
	}

	edge_dir_type scatter_edges(icontext_type& context, const vertex_type& vertex) const{
		if(state.update_self)
			return graphlab::NO_EDGES;
		else
			return graphlab::OUT_EDGES;
	}
	void scatter(icontext_type& context, const vertex_type& vertex, edge_type& edge) const{
		if(!state.update_self)
			context.signal(edge.target(), message_data(true, vertex.data().level + 1, vertex.id()));
	}
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

bool is_frontier(const graph_type::vertex_type& vertex) {
  return vertex.data().level == last_iter;
}

int vertex_outedge_num(const graph_type::vertex_type& vertex) {
  return 1 + vertex.num_out_edges();
}

int main(int argc, char** argv) {
	graphlab::mpi_tools::init(argc, argv);
	graphlab::distributed_control dc;
	
	std::string graph_dir;
	std::string saveprefix;
	std::string format;
	int source = -1;
	size_t powerlaw = 0;
	int threshold = -1;

	int updates = 0;
	float seconds = 0;

	// Parse command line options -----------------------------------------------
	graphlab::command_line_options clopts("Hybrid BFS algorithm.");
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
		"The source vertice, default 0");
	clopts.attach_option("format", format,
		"The graph format");
	clopts.attach_option("threshold", threshold,
		"threshold factor, default 2000");

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
	if(threshold <= 0) {
		dc.cout() << "No threhold factor provided or invalid. Use 2000" << std::endl;
		threshold = 2000;
	}

	// must call finalize before querying the graph
	graph.finalize();
	dc.cout() << "#vertices: " << graph.num_vertices()
	        << " #edges:" << graph.num_edges() << std::endl;

	threshold = graph.num_edges() / threshold;

	// Running The Engine -------------------------------------------------------
	graphlab::omni_engine<BFS_topdown> engine_topdown(dc, graph, "sync");
	graphlab::omni_engine<BFS_bottomup> engine_bottomup(dc, graph, "sync");

	// signal source
	engine_topdown.signal(source, message_data(true, 0, -1));
	engine_topdown.start();
	updates += engine_topdown.num_updates();
	seconds += engine_topdown.elapsed_seconds();

	for(last_iter = 0; ; last_iter ++){
		// get frontier vertex
		graphlab::vertex_set frontier = graph.select(is_frontier);
		// count number
		int frontier_outedge_num = graph.map_reduce_vertices<int>(vertex_outedge_num, frontier);
		dc.cout() << "#iter: " << last_iter << " threshold: " << threshold << " #frontier: " << frontier_outedge_num << std::endl;
		// if zero, finish
		if(frontier_outedge_num == 0) break;
		if (frontier_outedge_num < threshold){
			dc.cout() << "============ use top down ============" << std::endl;
			engine_topdown.signal_vset(frontier, message_data(false));
			engine_topdown.start();
			updates += engine_topdown.num_updates();
			seconds += engine_topdown.elapsed_seconds();
		} else {
			dc.cout() << "============ use bottom up ============" << std::endl;
			engine_bottomup.signal_all();
			engine_bottomup.start();
			updates += engine_bottomup.num_updates();
			seconds += engine_bottomup.elapsed_seconds();
		}
	}

	dc.cout() << updates << " updates." << std::endl;
	dc.cout() << "Finished in " << seconds << " seconds." << std::endl;


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