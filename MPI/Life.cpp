#include <mpi.h>
#include <vector>
#include <stdlib.h>
#include <unistd.h> 
#include <fstream>
#include <string>
#include <iostream>

enum TAG {UP, DOWN};

enum msg {WAIT, RUN, STOP, QUIT, PARAM, STATUS, ITERATION, TIME};

void set_random_table(std::vector<char>& table, int height, int width) {
    table.resize(height * width);
    for (int i = 0; i < height * width; ++i) {
        table[i] = rand() % 2;
    }
}

void set_csv_table(std::vector<char>& table, const char *csv_file, int& height, int& width) {
    std::ifstream in(csv_file);
    std::string line;
    height = 0;
    while(std::getline(in, line)) {
        height++;
        if (height == 1) {
            width = 0;
            for (int i = 0; i < line.size(); ++i) {
                if (i % 2 == 0) {
                    if (line[i] == '0') {
                        width++;
                        table.push_back(0);
                    } else if (line[i] == '1') {
                        width++;
                        table.push_back(1);
                    } else {
                        std::cerr << "Incorrect CSV field" << std::endl;
                        exit(1);
                    }
                } else if (line[i] != ',') {
                    std::cerr << "Incorrect CSV field" << std::endl;
                    exit(1);
                }
            }
        } else {
            if (line.size() + 1 < 2 * width) {
                std::cerr << "Incorrect CSV field" << std::endl;
                exit(1);
            }
            for (int i = 0; i < 2 * width - 1; ++i) {
                if (i % 2 == 0) {
                    if (line[i] == '0') {
                        table.push_back(0);
                    } else if (line[i] == '1') {
                        table.push_back(1);
                    } else {
                        std::cerr << "Incorrect CSV field" << std::endl;
                        exit(1);
                    }
                } else if (line[i] != ',') {
                    std::cerr << "Incorrect CSV field" << std::endl;
                    exit(1);
                }
            }
        }
    }
}

size_t calc_alive_neighbour_count(const std::vector<char>& table, int i, int j, int width) {
    size_t alive_neighbour_count = 0;
    alive_neighbour_count += table[(i - 1) * width + (j ? j - 1 : width - 1)];
    alive_neighbour_count += table[(i - 1) * width + j];
    alive_neighbour_count += table[(i - 1) * width + (j != width - 1 ? j + 1 : 0)];
    alive_neighbour_count += table[i * width + (j != width - 1 ? j + 1 : 0)];
    alive_neighbour_count += table[(i + 1) * width + (j != width - 1 ? j + 1 : 0)];
    alive_neighbour_count += table[(i + 1) * width + j];
    alive_neighbour_count += table[(i + 1) * width + (j ? j - 1 : width - 1)];
    alive_neighbour_count += table[i * width + (j ? j - 1 : width - 1)];
    return alive_neighbour_count;
}

void send_borders(std::vector<char>& table, int rank, int size, int height, int width) {
    MPI_Status status;
    int up_rank  = (rank != 1 ? rank - 1 : size - 1);
    int down_rank = (rank == size - 1 ? 1 : rank + 1);
    if (size > 2) {
        MPI_Sendrecv(&table[width], width, MPI_CHAR, up_rank, UP, &table[(height - 1) * width], width, MPI_CHAR, down_rank, UP, MPI_COMM_WORLD, &status);
        MPI_Sendrecv(&table[(height - 2) * width], width, MPI_CHAR, down_rank, DOWN, &table[0], width, MPI_CHAR, up_rank, DOWN, MPI_COMM_WORLD, &status);
    } else {
        for (int j = 0; j < width; ++j) {
            table[(height - 1) * width + j] = table[width + j];
            table[j] = table[(height - 2) * width + j];
        }
    }
}

void send_msg(msg message, int size) {
	MPI_Request request;
	MPI_Ibcast(&message, 1, MPI_INT, 0, MPI_COMM_WORLD, &request); 
}

void send_param(int cnt, int size) {
    MPI_Bcast(&cnt, 1, MPI_INT, 0, MPI_COMM_WORLD); 
}

void send_status(msg message, int size) {
	MPI_Request request;
    MPI_Ibcast(&message, 1, MPI_INT, 0, MPI_COMM_WORLD, &request); 
}

void print_status(std::vector<char>& table, int height, int width, int size) {
    int iteration;
	MPI_Bcast(&iteration, 1, MPI_INT, 1, MPI_COMM_WORLD);
    int mass_count[size];
	int mass_disp[size];
    for(int i = 0; i < size - 1; ++i) {
        mass_disp[i + 1] = height / (size - 1) * i;
        mass_count[i + 1] = height / (size - 1);
        if (i < height % (size - 1)) {
            mass_disp[i + 1] += i;
            mass_count[i + 1]++;
        } else {
            mass_disp[i + 1] += height % (size - 1);
        }
        mass_count[i + 1] *= width;
        mass_disp[i + 1] *= width;
    }
    mass_count[0] = 0;
    mass_disp[0] = 0;
    int *buf;
	MPI_Gatherv(buf, 0, MPI_CHAR, &table[0], mass_count, mass_disp, MPI_CHAR, 0, MPI_COMM_WORLD);
    std::cout << "Iteration: " << iteration << std::endl;
    for (int i = 0; i < height; ++i) {
        for(int j = 0; j < width; ++j) {
            std::cout << (table[i * width + j] ? 1 : 0) << ' ';
        }
        std::cout << std::endl;
    }
}

void print_iteration() {
    int iteration;
    MPI_Bcast(&iteration, 1, MPI_INT, 1, MPI_COMM_WORLD);
    std::cout << "Iteration: " << iteration << std::endl;
}

void iterate(std::vector<char>& table, std::vector<char>& next_table, int height, int width) {
    for(int i = 1; i < height - 1; ++i) {
        for(int j = 0; j < width; ++j) {
            size_t alive_neighbour_count = calc_alive_neighbour_count(table, i, j, width);
            if (table[i * width +j]) {
                if (alive_neighbour_count == 2 || alive_neighbour_count == 3) {
                    next_table[i * width +j] = 1;
                } else {
                    next_table[i * width +j] = 0;
                }
            } else {
                if (alive_neighbour_count == 3) {
                    next_table[i * width +j] = 1;
                } else {
                    next_table[i * width +j] = 0;
                }
            }
        }
    }
}

void send_table(std::vector<char>& table, int height, int width, int size) { // мастер отправляет работникам поле
	int mass_count[size]; // массив размеров блоков
	int mass_disp[size]; // массив сдвигов
    for(int i = 0; i < size - 1; ++i) { // заполняем их
        mass_disp[i + 1] = height / (size - 1) * i;
        mass_count[i + 1] = height / (size - 1);
        if (i < height % (size - 1)) {
            mass_disp[i + 1] += i;
            mass_count[i + 1]++;
        } else {
            mass_disp[i + 1] += height % (size - 1);
        }
        mass_count[i + 1] *= width;
        mass_disp[i + 1] *= width;
    }
    mass_count[0] = 0;
    mass_disp[0] = 0;
    int *recv;
    MPI_Bcast(&width, 1, MPI_INT, 0, MPI_COMM_WORLD); // бродкастим ширину поля и информацию о блоках
    MPI_Bcast(&mass_count[0], size, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&mass_disp[0], size, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Scatterv(&table[0], mass_count, mass_disp, MPI_CHAR, recv, 0, MPI_CHAR, 0, MPI_COMM_WORLD); // раскидываем потокам блоки поля
}

void init_table(std::vector<char>& table, int& height, int& width, int size, int rank) { 
    MPI_Status status;
    int mass_count[size];
	int mass_disp[size];
	int *buf;
    MPI_Bcast(&width, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&mass_count[0], size, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&mass_disp[0], size, MPI_INT, 0, MPI_COMM_WORLD);
    height = 2 + mass_count[rank] / width;
    table.resize(height * width);
    MPI_Scatterv(buf, mass_count, mass_disp, MPI_CHAR, &table[width], mass_count[rank], MPI_CHAR, 0, MPI_COMM_WORLD);
}

void master(int size) 
{
    std::vector<char> table;
    int width, height;
    bool started = false;
    msg st = WAIT; // изначально ждём команды
    std::string cmd;
    while(std::cin >> cmd)  {
        if (cmd == "START") {
            if (started) {
                std::cerr << "The game is already started. Try again." << std::endl;
                continue;
            }
            started = true;
            std::string info;
            std::cin >> info;
            if (info.find(".csv") != std::string::npos) {
            	set_csv_table(table, info.c_str(), height, width); 
                if (height == 0) {
                    std::cerr << "Empty table found in csv file. Try again." << std::endl;
                    continue;
                }
            } else {
                int m = 0;
                for (int i = 0; i < info.size(); ++i) {
                    if (info[i] < '0' || info[i] > '9') {
                        std::cerr << "Incorrect arguments of START command. Try again." << std::endl;
                        continue;
                    }
                    m = m * 10 + info[i] - '0';
                }
                int n;
                std::cin >> n;
                width = n;
                height = m;
                set_random_table(table, m, n);
            }
            send_table(table, height, width, size); // мастер отправляет работникам поле
        } else if (cmd == "RUN") {
            int cnt;
            std::cin >> cnt;
            st = RUN;
            send_msg(RUN, size); // отсылаем команду и число итераций
            send_param(cnt, size);
            sleep(5);
        } else if (cmd == "STATUS") {
            send_msg(STATUS, size);
            print_status(table, height, width, size);
        } else if (cmd == "STOP") {
            st = STOP;
            send_msg(STOP, size);
            print_iteration();
        } else if (cmd == "TIME") {
            send_msg(TIME, size);        
        } else if (cmd == "QUIT") {
            st = QUIT;
            send_msg(QUIT, size);
        } else {
            std::cerr << "Incorrect command. Try again" << std::endl;
        }
    }
}

void worker(int rank, int size) {
    msg st = WAIT; // изначально в режиме ожидания команды
    int width, height;
    int iteration = 0;
    int it_count = 0;
    double start_time, stop_time; 
    std::vector<char> odd_table;
    std::vector<char> even_table;
    init_table(even_table, height, width, size, rank);
    odd_table.resize(height * width);
    MPI_Request request;
    MPI_Status status;
    int message;
    MPI_Ibcast(&message, 1, MPI_INT, 0, MPI_COMM_WORLD, &request); 
    while (true) {
        int flag = 0;
        if (st == WAIT) {
            MPI_Test(&request, &flag, &status);
            while (!flag)
            {
                struct timespec tw = {0,1000000};
                struct timespec tr;
                nanosleep(&tw, &tr);
                MPI_Test(&request, &flag, &status);
            }
        } else {
            MPI_Test(&request, &flag, &status);
        }
        if (flag) {
            if (message == RUN) {
                st = RUN;
                start_time = MPI_Wtime();
                int add_it;
                MPI_Bcast(&add_it, 1, MPI_INT, 0, MPI_COMM_WORLD); 
                it_count += add_it;
            } else if (message == QUIT) {
                break;
            } else if (message == STOP) {
                if (st == RUN) {
                    st = WAIT;
                    it_count = iteration;
                    stop_time = MPI_Wtime();
                }
                MPI_Bcast(&iteration, 1, MPI_INT, 1, MPI_COMM_WORLD);
            } 
			else if (message == STATUS) {
                MPI_Bcast(&iteration, 1, MPI_INT, 1, MPI_COMM_WORLD);
                int *buf;
                int *mass_count;
                int *mass_disp;
                if (iteration % 2 == 0) {
                	MPI_Gatherv(&even_table[width], (height - 2) * width, MPI_CHAR, buf, mass_count, mass_disp, MPI_CHAR, 0, MPI_COMM_WORLD);
                } else {
        	    	MPI_Gatherv(&odd_table[width], (height - 2) * width, MPI_CHAR, buf, mass_count, mass_disp, MPI_CHAR, 0, MPI_COMM_WORLD);
                }
            } else if (message == ITERATION) {
                MPI_Bcast(&iteration, 1, MPI_INT, 1, MPI_COMM_WORLD);
            } else if (message == TIME) {
                if (st) {
                    std::cerr << "The game is still running. Stop it or wait till the end" << std::endl;
                } else {
                    std::cout << "The time is " << stop_time - start_time << " sec"  << std::endl;
                }
            }
            MPI_Ibcast(&message, 1, MPI_INT, 0, MPI_COMM_WORLD, &request); 
        } 
        if (iteration < it_count) {
            send_borders(iteration % 2 ? odd_table : even_table, rank, size, height, width);
            iterate(iteration % 2 ? odd_table : even_table, iteration % 2 ? even_table : odd_table, height, width);
            iteration++;
        } else {
            if (st == RUN) {
                stop_time = MPI_Wtime();
                st = WAIT;
            }
        }
    }
}

int main(int argc, char **argv) {
    int rank, size;
    MPI_Comm comm;
    int status = MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if (size < 2) {
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    if (rank == 0) {
        master(size);
    } else {
        worker(rank, size);
    }
    MPI_Finalize();
    return 0;
}
