#include <graphlab.hpp>
#include <string>
#include <stdlib.h>

struct web_page{
	std::string pagename;
	double pagerank;
	web_page(): pagerank(0.0){}
	explicit web_page(std::string name):pagename(name), pagerank(0.0){}

	void load(graphlab::iarchive& iarc){
		iarc >> pagename >> pagerank;
	}

	void save(graphlab::oarchive& oarc) const{
		oarc << pagename <<pagerank;
	}
};

typedef graphlab::distributed_graph<web_page, graphlab::empty> graph_type;

bool line_parser(graph_type& graph,
	const std::string& filename,
	const std::string& textline){
	std::stringstream strm(textline);
	graphlab::vertex_id_type vid;
	std::string pagename;
	strm >> vid;
	strm >> pagename;
	graph.add_vertex(vid, web_page(pagename));
	while(1){
		graphlab::vertex_id_type other_vid;
		strm >> other_vid;
		if(strm.fail())
			return true;
		graph.add_edge(vid, other_vid);
	}
	return true;
}

class pagerank_program: public graphlab::ivertex_program<graph_type, double>, public graphlab::IS_POD_TYPE{
private:
	bool perform_scatter;
public:
	edge_dir_type gather_edges(icontext_type& context, const vertex_type& vertex) const{
		return graphlab::IN_EDGES;
	}

	double gather(icontext_type& context, const vertex_type& vertex, edge_type& edge) const{
		return edge.source().data().pagerank / edge.source().num_out_edges();
	}

	void apply(icontext_type& context, vertex_type& vertex, const gather_type& total){
		double newval = total * 0.85 + 0.15;
		double prevval = vertex.data().pagerank;
		vertex.data().pagerank = newval;
		perform_scatter = (std::fabs(prevval - newval) > 1E-3);
	}

	edge_dir_type scatter_edges(icontext_type& context, const vertex_type& vertex) const{
		if(perform_scatter)
			return graphlab::OUT_EDGES;
		return graphlab::NO_EDGES;
	}
	void scatter(icontext_type& context, const vertex_type& vertex, edge_type& edge) const{
		context.signal(edge.target());
	}
};

class graph_writer{
public:
	std::string save_vertex(graph_type::vertex_type v){
		std::stringstream strm;
		strm << v.data().pagename << '\t' << v.data().pagerank << '\n';
		return strm.str();
	} 
	std::string save_edge(graph_type::edge_type e){
		return "";
	}
};

int main(int argc, char** argv) {
	graphlab::mpi_tools::init(argc, argv);
	graphlab::distributed_control dc;

	graph_type graph(dc);
	graph.load("graph.txt", line_parser);
	graph.finalize();
	dc.cout() << "#vertices: " << graph.num_vertices() << " #edges:" << graph.num_edges() << std::endl;

	graphlab::omni_engine<pagerank_program> engine(dc, graph, "sync");
	engine.signal_all();
	engine.start();

	graph.save("output", graph_writer(), false, true, false);
	graphlab::mpi_tools::finalize();
}