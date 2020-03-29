/*
 * Filename: d:\Workspace\DataBase\db\src\file.cc
 * Path: d:\Workspace\DataBase\db\src
 * Created Date: Saturday, March 28th 2020, 10:49:23 am
 * Author: Honor
 *
 * Copyright (c) 2020 Your Company
 */

#include <db/file.h>

namespace db {
//转化为宽字节
LPCWSTR stringToLPCWSTR(std::string orig)
{
    size_t origsize = orig.length() + 1;
    const size_t newsize = 100;
    size_t convertedChars = 0;
    wchar_t *wcstring =
        (wchar_t *) malloc(sizeof(wchar_t) * (orig.length() - 1));
    mbstowcs_s(&convertedChars, wcstring, origsize, orig.c_str(), _TRUNCATE);
    return wcstring;
}
//写文件
void Write(char *buffer, DWORD contentLen)
{
    HANDLE pFile;
    char *tmpBuf;
    DWORD dwBytesWrite, dwBytesToWrite;

    pFile = CreateFile(
        stringToLPCWSTR(filePath),
        GENERIC_WRITE,
        0,
        NULL,
        CREATE_ALWAYS, //总是创建文件
        FILE_ATTRIBUTE_NORMAL,
        NULL);

    if (pFile == INVALID_HANDLE_VALUE) {
        printf("create file error!\n");
        CloseHandle(pFile);
    }

    dwBytesToWrite = contentLen;
    dwBytesWrite = 0;

    tmpBuf = buffer;

    do { //循环写文件，确保完整的文件被写入

        WriteFile(pFile, tmpBuf, dwBytesToWrite, &dwBytesWrite, NULL);

        dwBytesToWrite -= dwBytesWrite;
        tmpBuf += dwBytesWrite;

    } while (dwBytesToWrite > 0);

    CloseHandle(pFile);
}
//读文件
char *Read()
{
    HANDLE pFile;
    DWORD fileSize;
    char *buffer, *tmpBuf;
    DWORD dwBytesRead, dwBytesToRead, tmpLen;

    pFile = CreateFile(
        stringToLPCWSTR(filePath),
        GENERIC_READ,
        FILE_SHARE_READ,
        NULL,
        OPEN_EXISTING, //打开已存在的文件
        FILE_ATTRIBUTE_NORMAL,
        NULL);

    if (pFile == INVALID_HANDLE_VALUE) {
        printf("open file error!\n");
        CloseHandle(pFile);
        return NULL;
    }

    fileSize = GetFileSize(pFile, NULL); //得到文件的大小

    buffer = (char *) malloc(fileSize);
    ZeroMemory(buffer, fileSize);
    dwBytesToRead = fileSize;
    dwBytesRead = 0;
    tmpBuf = buffer;

    do { //循环读文件，确保读出完整的文件
        ReadFile(pFile, tmpBuf, dwBytesToRead, &dwBytesRead, NULL);
        if (dwBytesRead == 0) break;
        dwBytesToRead -= dwBytesRead;
        tmpBuf += dwBytesRead;
    } while (dwBytesToRead > 0);

    free(buffer);
    CloseHandle(pFile);
    return buffer;
}
} // namespace db