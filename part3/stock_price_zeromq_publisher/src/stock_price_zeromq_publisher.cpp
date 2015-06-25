#include <zmq.hpp>

#include <string>
#include <sstream>
#include <iostream>
#include <random>
#include <vector>
#include <chrono>
#include <thread>


struct StockData {
	StockData(int id, double mean, double std_deviation) :
		id(id), mean(mean), std_deviation(std_deviation) {}
	int id = -1;
	double mean = 0.0;
	double std_deviation = 0.0;
};

int main (int argc, char **argv) {
	using std::string;
	using std::cout;
	using std::cerr;
	using std::endl;

	using normal_dist = std::normal_distribution<double>;
	using uniform_int_dist = std::uniform_int_distribution<int>;

	if (3 != argc ) {
		cerr << "usage <zeroMQurl> <topic>" << endl;
		return 1;
	}

	const std::chrono::milliseconds update_delay(400);
	const std::vector<StockData> stocks = {
			{1,   50.0,  0.30},
			{2,   70.0,  0.35},
			{3,    4.5,  0.60},
			{4,  102.2,  0.40} };

	std::default_random_engine choose_stock_generator;
	uniform_int_dist choose_stock_distribution(0, stocks.size() - 1);

	std::default_random_engine stock_price_generator;

	const string url(argv[1]);
	const string topic(argv[2]);

	cout << "zmq url:" << url << endl;
	cout << "topic:" << topic << endl;

    zmq::context_t context(1);
    zmq::socket_t socket(context, ZMQ_PUB);
    socket.bind(url.c_str());

    while (true) {

    	const StockData& stock =
    			stocks.at(choose_stock_distribution(choose_stock_generator));
    	normal_dist price_distribution(stock.mean, stock.std_deviation);

        std::stringstream id_and_price;
    	id_and_price << stock.id << "," <<
    			price_distribution(stock_price_generator);

        zmq::message_t topic_msg(topic.size());
        memcpy(topic_msg.data(), topic.data(), topic.size());
        const bool topic_send_rv = socket.send(topic_msg, ZMQ_SNDMORE);


        const auto& id_and_price_str = id_and_price.str();
        zmq::message_t id_and_price_msg(id_and_price_str.size());

        memcpy(id_and_price_msg.data(), id_and_price_str.data(),
        	   id_and_price_str.size());
        const bool price_send_rv = socket.send(id_and_price_msg);

        cout << "sent: " << id_and_price_str << endl;
        cout << "send statuses: " << topic_send_rv <<  " " << price_send_rv << endl;

    	std::this_thread::sleep_for(update_delay);
    }

    return 0;
}
