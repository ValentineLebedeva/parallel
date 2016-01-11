#include <mpi.h>
#include <vector>
#include <stdlib.h>
#include <unistd.h> 
#include <fstream>
#include <string>
#include <iostream>

enum TAG {UP, DOWN, STATE, PARAM, TIME, STATUS, ITERATION, HEIGHT, WIDTH};

enum state {WAIT, RUN, STOP, QUIT};

void set_random_table(std::vector<std::vector<char> >& table, int height, int width) {
    table.resize(height);
    for (int i = 0; i < height; ++i) {
        table[i].resize(width);
    }
    srand(0);
    for (int i = 0; i < height; ++i) {
        for (int j = 0; j < width; ++j) {
            table[i][j] = rand() % 2;
        }
    }
}

void set_csv_table(std::vector<std::vector<char> >& table, const char *csv_file) {
    std::ifstream in(csv_file);
    std::string line;
    int height = 0;
    int width;
    while(std::getline(in, line)) {
        height++;
        std::vector<char> row;
        if (height == 1) {
            width = 0;
            for (int i = 0; i < line.size(); ++i) {
                if (i % 2 == 0) {
                    if (line[i] == '0') {
                        width++;
                        row.push_back(0);
                    } else if (line[i] == '1') {
                        width++;
                        row.push_back(1);
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
                        row.push_back(0);
                    } else if (line[i] == '1') {
                        row.push_back(1);
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
        table.push_back(row);
    }
}

size_t calc_alive_neighbour_count(const std::vector<std::vector<char> >& table, int i, int j, int width) {
    size_t alive_neighbour_count = 0;
    alive_neighbour_count += table[i - 1][j ? j - 1 : width - 1];
    alive_neighbour_count += table[i - 1][j];
    alive_neighbour_count += table[i - 1][j != width - 1 ? j + 1 : 0];
    alive_neighbour_count += table[i][j != width - 1 ? j + 1 : 0];
    alive_neighbour_count += table[i + 1][j != width - 1 ? j + 1 : 0];
    alive_neighbour_count += table[i + 1][j];
    alive_neighbour_count += table[i + 1][j ? j - 1 : width - 1];
    alive_neighbour_count += table[i][j ? j - 1 : width - 1];
    return alive_neighbour_count;
}

void send_borders(std::vector<std::vector<char> >& table, int rank, int size, int height, int width) { // ��� ��� �� �������� � MPI, � ������� �������� ���� ������, � ��� ����� �������� ��������� ������ � �������
    MPI_Status status;
    int up_rank  = (rank == 1 ? size - 1 : rank - 1); // ��������� �������
    int down_rank = (rank == size - 1 ? 1 : rank + 1);
    if (size > 2) {
        MPI_Sendrecv(&table[1].front(), width, MPI_CHAR, up_rank, UP, &table[height - 1].front(), width, MPI_CHAR, down_rank, UP, MPI_COMM_WORLD, &status); // ��������� �� �������� �������
        MPI_Sendrecv(&table[height - 2].front(), width, MPI_CHAR, down_rank, DOWN, &table[0].front(), width, MPI_CHAR, up_rank, DOWN, MPI_COMM_WORLD, &status);
    } else {
        for (int j = 0; j < width; ++j) {
            table[height - 1][j] = table[1][j];
            table[0][j] = table[height - 2][j];
        }
    }
}

void send_state(state st, int size) {
    for(int i = 1; i < size; ++i) {
        MPI_Send(&st, 1, MPI_INT, i, STATE, MPI_COMM_WORLD);
    }
}

void send_param(int cnt, int size) {
    for(int i = 1; i < size; ++i) {
        MPI_Send(&cnt, 1, MPI_INT, i, PARAM, MPI_COMM_WORLD);
    }
}

void send_status(state st, int size) {
    for(int i = 1; i < size; ++i) {
        MPI_Send(&st, 1, MPI_INT, i, STATUS, MPI_COMM_WORLD);
    }
}

void send_time(state st, int size) {
    for(int i = 1; i < size; ++i) {
        MPI_Send(&st, 1, MPI_INT, i, TIME, MPI_COMM_WORLD);
    }
}

void print_status(std::vector<std::vector<char> >& table, int height, int width, int size) { // ����� ������� � �������
    int iteration;
    MPI_Status status;
    MPI_Recv(&iteration, 1, MPI_INT, 1, ITERATION, MPI_COMM_WORLD, &status); // ��������� ����� ��������
    for(int i = 0; i < size - 1; ++i) { // �� ������� �������� ������� ��������� ��������
        int start_pos = height / (size - 1) * i;
        int block_size = height / (size - 1);
        if (i < height % (size - 1)) {
            start_pos += i;
            block_size++;
        } else {
            start_pos += height % (size - 1);
        }
        for (int j = start_pos; j < start_pos + block_size; ++j) {
            MPI_Recv(&table[j].front(), width, MPI_CHAR, i + 1, j - start_pos, MPI_COMM_WORLD, &status);
        }
    }
    std::cout << "Iteration: " << iteration << std::endl; // ������ ���� �� �������
    for (int i = 0; i < height; ++i) {
        for(int j = 0; j < width; ++j) {
            std::cout << (table[i][j] ? 1 : 0) << ' ';
        }
        std::cout << std::endl;
    }
}

void print_iteration() { // ����� ��������
    int iteration;
    MPI_Status status;
    MPI_Recv(&iteration, 1, MPI_INT, 1, ITERATION, MPI_COMM_WORLD, &status); // ��������� ����� �������� �� ���������
    std::cout << "Iteration: " << iteration << std::endl;
}

void iterate(std::vector<std::vector<char> >& table, std::vector<std::vector<char> >& next_table, int height, int width) { // ���������� ����� ��������
    for(int i = 1; i < height - 1; ++i) {
        for(int j = 0; j < width; ++j) {
            size_t alive_neighbour_count = calc_alive_neighbour_count(table, i, j, width);
            if (table[i][j]) {
                if (alive_neighbour_count == 2 || alive_neighbour_count == 3) {
                    next_table[i][j] = 1;
                } else {
                    next_table[i][j] = 0;
                }
            } else {
                if (alive_neighbour_count == 3) {
                    next_table[i][j] = 1;
                } else {
                    next_table[i][j] = 0;
                }
            }
        }
    }
}

void send_table(std::vector<std::vector<char> >& table, int height, int width, int size) { // ��������� ���� �� ��������-������� ����������
    for(int i = 0; i < size - 1; ++i) { // ������� ������ ��� ������� ��������
        int start_pos = height / (size - 1) * i; // ������ ������� �����
        int block_size = height / (size - 1); // ������ ������� �����
        if (i < height % (size - 1)) {
            start_pos += i;
            block_size++;
        } else {
            start_pos += height % (size - 1);
        }
        MPI_Send(&block_size, 1, MPI_INT, i + 1, HEIGHT, MPI_COMM_WORLD);  // ��� �������� ������ ��� �����
        MPI_Send(&width, 1, MPI_INT, i + 1, WIDTH, MPI_COMM_WORLD); // ��� ��� ������ ������ ����
        for (int j = start_pos; j < start_pos + block_size; ++j) {
            MPI_Send(&table[j].front(), table[j].size(), MPI_CHAR, i + 1, j, MPI_COMM_WORLD); // �������� ��������� ���� ���� �������
        }
    }
}

void init_table(std::vector<std::vector<char> >& table, int& height, int& width, int size) { // ���������� ��������� ���� �� ������� �����������
    MPI_Status status;
    MPI_Recv(&height, 1, MPI_INT, 0, HEIGHT, MPI_COMM_WORLD, &status); // ��������� ������� ������ �����
    MPI_Recv(&width, 1, MPI_INT, 0, WIDTH, MPI_COMM_WORLD, &status);
    height += 2; // ������ ��� ������ ������� - ��� ����� ������
    table.resize(height);
    for(int i = 0; i < height; ++i) {
        table[i].resize(width);
    }
    for(int i = 1; i < height - 1; ++i) {
        MPI_Recv(&table[i].front(), width, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status); // ����� ��������� ��������� ����
    }
}

void master(int size) 
{
    std::vector<std::vector<char> > table;
    int width, height;
    bool started = false; // �������� �� ������, ��� �� ������ �����
    state st = WAIT;
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
            if (info.find(".csv") != std::string::npos) { // ���� ���� �� �����
                set_csv_table(table, info.c_str());
                height = table.size();
                if (height == 0) {
                    std::cerr << "Empty table found in csv file. Try again." << std::endl;
                    continue;
                }
                width = table[0].size();
            } else { // �������� ���������� ����
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
            send_table(table, height, width, size); // �������-������ ���� �������� � ��������� ������ � ��� � ����� �������
        } else if (cmd == "RUN") {
            int cnt; // ����� ��������
            std::cin >> cnt;
            st = RUN;
            send_state(st, size); // ��� ������� ���� ������� ���������
            send_param(cnt, size); // ��� ����� �������� ���� ������� ���������
            sleep(5); // ����� ���� (??)
        } else if (cmd == "STATUS") {
            send_status(st, size); // �������� ������ �������
            print_status(table, height, width, size); // ������� ������ � �������
        } else if (cmd == "STOP") {
            st = STOP;
            send_state(st, size);
            print_iteration(); // ������� ��������, �� ������� ������
        } else if (cmd == "TIME") { // ����� ��������� ����� ������ ����������� RUN
            send_time(st, size);        
        } else if (cmd == "QUIT") {
            st = QUIT;
            send_state(st, size);
            break;
        } else {
            std::cerr << "Incorrect command. Try again" << std::endl;
        }
    }
}

void worker(int rank, int size) {
    state st = WAIT; // ��������� ���������
    int width, height;
    int iteration = 0; // ������� ��������
    int it_count = 0; // ����� ��������
    double start_time, stop_time; // ������, ��� �������� (?)
    std::vector<std::vector<char> > odd_table;
    std::vector<std::vector<char> > even_table;
    init_table(even_table, height, width, size); // �������� ���� � ��� ��������� �� �������
    odd_table.resize(height);
    for(int i = 0; i < height; ++i) {
        odd_table[i].resize(width);
    }
    MPI_Request request;
    MPI_Status status;
    int message;
    MPI_Irecv(&message, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &request); // ������ ������������� ���� �������
    while (true) {
        int flag = 0;
        if (st == WAIT) { // ����������� ���� ��������� ���������
            MPI_Test(&request, &flag, &status); // ��������, ������ �� �������
            while (!flag) // ���� �� ������, ���, ���� �����
            {
                struct timespec tw = {0,1000000};
                struct timespec tr;
                nanosleep(&tw, &tr);
                MPI_Test(&request, &flag, &status);
            }
        } else {
            MPI_Test(&request, &flag, &status); 
        }
        if (flag) { // ����� ��������� ������
            int tag = status.MPI_TAG; // �������, ��� ������ ������
            if (tag == STATE) { // ������ �������, �������, ����� ������
                if (message == RUN) {
                    st = RUN;
                    start_time = MPI_Wtime(); // �������� �����
                    int add_it;
                    MPI_Recv(&add_it, 1, MPI_INT, 0, PARAM, MPI_COMM_WORLD, &status); // ��������� ����� ��������
                    it_count += add_it;
                } else if (message == QUIT) { // �����, ��������� �� �����
                    break;
                } else if (message == STOP) { // ����� ���������������
                    if (st == RUN) {
                        st = WAIT; // ����� ������ ������ � ����� �������� ���������
                        it_count = iteration; // �������, ��� ������ �������� ���
                        stop_time = MPI_Wtime(); // �������� ����� ���������
                    } 
                    if (rank == 1) {
                        MPI_Send(&iteration, 1, MPI_INT, 0, ITERATION, MPI_COMM_WORLD); // ������ ������� ��� ������� ����� �������� 
                    }
                }
            } else if (tag == STATUS) {
                if (rank == 1) {
                    MPI_Send(&iteration, 1, MPI_INT, 0, ITERATION, MPI_COMM_WORLD); // ������ ������� ��� ������� ����� ��������
                }
                for(int i = 1; i < height - 1; ++i) { // �������� ������� ��������
                    if (iteration % 2 == 0) {
                        MPI_Send(&even_table[i].front(), even_table[i].size(), MPI_CHAR, 0, i - 1, MPI_COMM_WORLD);
                    } else {
                        MPI_Send(&odd_table[i].front(), odd_table[i].size(), MPI_CHAR, 0, i - 1, MPI_COMM_WORLD);
                    }
                }
            } else if (tag == ITERATION) { // ������ �� ����� ��������, ������ (???)
                if (rank == 1) {
                    MPI_Send(&iteration, 1, MPI_INT, 0, ITERATION, MPI_COMM_WORLD);
                }
            } else if (tag == TIME) { // ������ �� ��������� ������� ������
                if (st) {
                    std::cerr << "The game is still running. Stop it or wait till the end" << std::endl; // ������ ��� �� �����������
                } else {
                    std::cout << "The time is " << stop_time - start_time << " sec"  << std::endl;
                }
            }
            MPI_Irecv(&message, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &request); // ������������� ���� ������-������ ������ ���������
        } 
        if (iteration < it_count) { // ���� �� ��� ������ RUN
            send_borders(iteration % 2 ? odd_table : even_table, rank, size, height, width); // ������������ ��������� �� �������� ������
            iterate(iteration % 2 ? odd_table : even_table, iteration % 2 ? even_table : odd_table, height, width); // ������ ����� ��������� ��������
            iteration++;
        } else {
            if (st == RUN) { // ���� �� �������� RUN
                stop_time = MPI_Wtime(); // ���������� ����� ���������
                st = WAIT; // ����� ������ � ����� �������� �������
            }
        }
    }
}

int main(int argc, char **argv) {
    int rank, size;
    MPI_Comm comm;
    for (int i = 0; i < argc; ++i) {
        std::cout << argv[i] << std::endl;
    }
    int status = MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if (size < 2) {
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    const char *file_name = argv[1];
    if (rank == 0) {
        master(size);
    } else {
        worker(rank, size);
    }
    MPI_Finalize();
    return 0;
}
