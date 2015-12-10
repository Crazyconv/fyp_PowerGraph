#include <graphlab.hpp>
#include <string>
#include <stdlib.h>

// global variable
float threshold = 0.1;
int unvisited_num = 0;

struct vertex_data : graphlab::IS_POD_TYPE {
	int dist;
	int parent;
	vertex_data(int dist = -2, int parent = -1) : dist(dist), parent(parent) {}
};

// used in bottom_up
struct gather_data : graphlab::IS_POD_TYPE {
	int dist;
	int id;
	gather_data(int dist = -2, int id = -1): dist(dist), id(id) {}

	gather_data & operator+=(const gather_data & other){
		if(dist <= 0 || (other.dist > 0 && other.dist < dist)){
			dist = other.dist;
			id = other.id;
		}
		return *this;
	}
};

// used in top_down
struct message_data : graphlab::IS_POD_TYPE {
	int dist;
	int id;
	message_data(int dist = -2, int id = -1): dist(dist), id(id) {}

	message_data & operator+=(const message_data & other){
		return *this;
	}
};

typedef graphlab::empty edge_data;
typedef graphlab::distributed_graph<vertex_data, edge_data> graph_type;

class BFS_bottomup: public graphlab::ivertex_program<graph_type, gather_data, graphlab::empty>, public graphlab::IS_POD_TYPE{
public:
	edge_dir_type gather_edges(icontext_type& context, const vertex_type& vertex) const{
		if(vertex.data().dist >= 0){
			return graphlab::NO_EDGES;
		}
		else
			return graphlab::IN_EDGES;
	}

	gather_data gather(icontext_type& context, const vertex_type& vertex, edge_type& edge) const{
		return gather_data(edge.source().data().dist + 1, edge.source().id());
	}

	void apply(icontext_type& context, vertex_type& vertex, const gather_type& total){
		if(vertex.data().dist < 0){
			if (((gather_data)total).dist > 0){
				vertex.data().dist = total.dist;
				vertex.data().parent = total.id;
			} else {
				context.signal(vertex);
			}
		} 
	}

	edge_dir_type scatter_edges(icontext_type& context, const vertex_type& vertex) const{
		return graphlab::NO_EDGES;
	}
};

class BFS_topdown: public graphlab::ivertex_program<graph_type, graphlab::empty, message_data>, public graphlab::IS_POD_TYPE{
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
		if(vertex.data().dist < 0 && state.dist >= 0){
			vertex.data().dist = state.dist;
			vertex.data().parent = state.id;
			new_frontier = true;
		}
	}

	edge_dir_type scatter_edges(icontext_type& context, const vertex_type& vertex) const{
		if(new_frontier)
			return graphlab::OUT_EDGES;
		else
			return graphlab::NO_EDGES;
	}
	void scatter(icontext_type& context, const vertex_type& vertex, edge_type& edge) const{
		context.signal(edge.target(), message_data(vertex.data().dist + 1, vertex.id()));
	}
};

typedef graphlab::omni_engine<BFS_topdown> topdown_engine_type;
typedef graphlab::omni_engine<BFS_bottomup> bottomup_engine_type;

class graph_writer{
public:
	std::string save_vertex(graph_type::vertex_type v){
		std::stringstream strm;
		strm << v.id() << "\t" << v.data().dist << "\t" << v.data().parent << "\n";
		return strm.str();
	} 
	std::string save_edge(graph_type::edge_type e){
		return "";
	}
};

int is_visited(topdown_engine_type::icontext_type& context, const graph_type::vertex_type& vertex){
    return (vertex.data().dist >= 0)? 1:0;
}

void count_visited(topdown_engine_type::icontext_type& context, int total){
    if(total >= threshold)
        context.stop();
}

int is_unvisited(bottomup_engine_type::icontext_type& context, const graph_type::vertex_type& vertex){
    return (vertex.data().dist < 0)? 1:0;
}

void count_unvisited(bottomup_engine_type::icontext_type& context, int total){
    if(total == unvisited_num){
        context.stop();
    } else {
        unvisited_num = total;
    }
}

int main(int argc, char** argv) {
	graphlab::mpi_tools::init(argc, argv);
	graphlab::distributed_control dc;
	
	std::string graph_dir;
	std::string saveprefix;
	std::string format;
	int source = -1;
	size_t powerlaw = 0;

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
		"threshold factor, 0.1");

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
		dc.cout() << "No threhold factor provided or invalid. Use 0.1" << std::endl;
		threshold = 0.1;
	}

	// must call finalize before querying the graph
	graph.finalize();
	dc.cout() << "#vertices: " << graph.num_vertices()
	        << " #edges:" << graph.num_edges() << std::endl;

	threshold *= graph.num_vertices();

	// Running The Engine -------------------------------------------------------
	topdown_engine_type engine_topdown(dc, graph, "sync");
	bottomup_engine_type engine_bottomup(dc, graph, "sync");

	// global aggregator
	engine_topdown.add_vertex_aggregator<int>("count_visited_num", is_visited, count_visited);
    engine_topdown.aggregate_periodic("count_visited_num",10);

	// signal source
	engine_topdown.signal(source, message_data(0, source));
	engine_topdown.start();
	updates += engine_topdown.num_updates();
	seconds += engine_topdown.elapsed_seconds();

	// switch to bottom up
	engine_bottomup.add_vertex_aggregator<int>("count_unvisited_num", is_unvisited, count_unvisited);
    engine_bottomup.aggregate_periodic("count_unvisited_num",10);
	engine_bottomup.signal_all();
	engine_bottomup.start();
	updates += engine_bottomup.num_updates();
	seconds += engine_bottomup.elapsed_seconds();

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