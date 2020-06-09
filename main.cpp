#include <vector>
#include <string>
#include<windows.h>
#include <fstream>
#include <iostream>
using namespace std;
struct RowData
{
	int id;
	string name;
	string number;
	bool sex;
	string email;
};
void initial();
void insert(std::vector<RowData> rows);//插入数据默认按照主键升序
std::vector<RowData> query(string sql); //查询数据默认按照主键升序
void del(string sql);
void update(string sql);