/*
 * GraphRead.c
 *
 * Author: Sarwar Alam
 *
 */

#include <zmq.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <time.h>

#define TRUE  1
#define FALSE 0
#define ZeroMQBUFFSIZE 255
#define Filename "AirportDB.csv"
#define Dataset  "DataSet.csv"
#define RECORDNUM 6260
#define LINEBUFF 1000
#define MAXID   16598


typedef struct file_record_t
{
        int     airportID;
        char    airportName[ZeroMQBUFFSIZE];
        double  latitude;
        double  longitude;
} file_record;

file_record all_airport_data[RECORDNUM];


typedef struct big_record_t
{
        char    airportName[ZeroMQBUFFSIZE];
        long    latitude;
        long    longitude;
} big_record;
big_record all_data[MAXID];


typedef struct dataset_record_t
{
        char    date[ZeroMQBUFFSIZE];
        char    airport_ID_origin[ZeroMQBUFFSIZE];
        char    airport_ID_dest[ZeroMQBUFFSIZE];
        char    dep_time[ZeroMQBUFFSIZE];
        char    arr_time[ZeroMQBUFFSIZE];
        char    distance[ZeroMQBUFFSIZE];
} dataset_record;



typedef struct zmq_record_t
{
        long  lat_origin;
        long  long_origin;
        long  dep_time;

        long  lat_dest;
        long  long_dest;
        long  arr_time;

        long  airport_ID_origin;
        long  airport_ID_dest;

        double  distance;
} zmq_record;




int main(int argc, char **argv)
{
    int i;
    int rv = 0;
    char line[LINEBUFF];
    char str[ZeroMQBUFFSIZE];
    char connectTo[ZeroMQBUFFSIZE];
    char delim[2] = ",";
    char delimDash[2] = "-";
    char delimQuote[2] = "\"";
    char* token;
    double delay = 0;
    strcpy(connectTo,"127.0.0.1:11117");
    file_record data;
    zmq_record  record;
    dataset_record  entry;
    struct tm   myclock;
    struct tm   myclock2;


    if(argc >= 2)
    {
        if( !strcmp(argv[1], "help") || !strcmp(argv[1], "-help") || !strcmp(argv[1], "-h") )
        {
            printf("usage: %s [IP] [DELAY]\n",argv[0]);
            exit(0);
        }
        strcpy(connectTo,argv[1]);
    }
    if(argc >= 3)
    {
        sscanf(argv[2], "%lf", &delay);
    }

    void *context = zmq_ctx_new ();
    void *push_socket = zmq_socket (context, ZMQ_PUSH);

    sprintf(str,"tcp://%s",connectTo);
    printf("Binding socket to %s\n",str);
    rv = zmq_bind (push_socket, str);
    assert (rv == 0);


    // Storing all airports data
    printf("Reading Airport Database file\n");
    FILE* fp = fopen(Filename,"r");
    if(fp==NULL)
    {
        printf("Cannot open file %s\n",Filename);
        exit(1);
    }
    i=0;
    while(fgets(line,LINEBUFF,fp)!=NULL)
    {
        token = strtok(line,delim);
        data.airportID = atoi(token);

        token = strtok(NULL,delim);
        strcpy(data.airportName,token);

        token = strtok(NULL,delim);
        data.latitude = atof(token);

        token = strtok(NULL,delim);
        data.longitude = atof(token);

        //printf("%10d: %d,%s,%lf,%lf\n",i,data.airportID,data.airportName,data.latitude,data.longitude);

        all_airport_data[i] = data;

        strcpy(all_data[data.airportID].airportName, data.airportName);
        all_data[data.airportID].latitude    = (long)((data.latitude  + 360)*1000);      // Transformation
        all_data[data.airportID].longitude   = (long)((data.longitude + 360)*1000);

        i++;
    }
    fclose(fp);


    printf("Reading from Data File\n");
    fp = fopen(Dataset,"r");
    if(fp==NULL)
    {
        printf("Cannot open file %s\n",Dataset);
        exit(1);
    }
    fgets(line,LINEBUFF,fp);    // Omit first line
    while(TRUE) // Infinite loop
    {
        if(fgets(line,LINEBUFF,fp)==NULL)
        {
            // Rewind to the start of the file (Or we can exit using "break")
            //break;
            fclose(fp);
            fp = fopen(Dataset,"r");
            if(fp==NULL)
            {
                printf("Cannot open file %s\n",Dataset);
                exit(1);
            }
            fgets(line,LINEBUFF,fp);    // Omit first line
            if(fgets(line,LINEBUFF,fp)==NULL)
                break;
        }

        memset(&myclock,0,sizeof(struct tm));
        memset(&myclock2,0,sizeof(struct tm));

        token = strtok(line,delim);
        if(token) strcpy(entry.date,token);
        else continue;

        token = strtok(NULL,delim);
        if(token) strcpy(entry.airport_ID_origin,token);
        else continue;

        token = strtok(NULL,delim);
        if(token) strcpy(entry.airport_ID_dest,token);
        else continue;

        token = strtok(NULL,delim);
        if(token) strcpy(entry.dep_time,token);
        else continue;

        token = strtok(NULL,delim);
        if(token) strcpy(entry.arr_time,token);
        else continue;

        token = strtok(NULL,delim);
        if(token) strcpy(entry.distance,token);
        else continue;


        // Year
        token = strtok(entry.date,delimDash);
        if(token) strcpy(str,token);
        else continue;
        myclock.tm_year = atoi(token) - 1900;
        myclock2.tm_year = atoi(token) - 1900;
        //printf("Year %s ",token);

        // Month
        token = strtok(NULL,delimDash);
        if(token) strcat(str,token);
        else continue;
        myclock.tm_mon = atoi(token) - 1;
        myclock2.tm_mon = atoi(token) - 1;
        //printf("Day %s ",token);

        // Day
        token = strtok(NULL,delimDash);
        if(token) strcat(str,token);
        else continue;
        myclock.tm_mday = atoi(token);
        myclock2.tm_mday = atoi(token);
        //printf("Month %s ",token);

        strcpy(entry.date,str);

        token = strtok(entry.dep_time,delimQuote);
        if(token) strcpy(entry.dep_time,token);
        else continue;


        token = strtok(entry.arr_time,delimQuote);
        if(token) strcpy(entry.arr_time,token);
        else continue;

        record.airport_ID_origin = atol(entry.airport_ID_origin);
        record.airport_ID_dest   = atol(entry.airport_ID_dest);

        record.lat_origin  = all_data[atoi(entry.airport_ID_origin)].latitude;
        record.long_origin = all_data[atoi(entry.airport_ID_origin)].longitude;

        //strcpy(str,entry.date);
        //strcat(str,entry.dep_time);
        //record.dep_time    = atol(str);
        myclock.tm_hour  = atoi(entry.dep_time)/100;
        myclock.tm_min   = atoi(entry.dep_time)%100;
        record.dep_time  = mktime(&myclock);
        //printf("Hour %02d Min %02d ",myclock.tm_hour,myclock.tm_min);

        record.lat_dest  = all_data[atoi(entry.airport_ID_dest)].latitude;
        record.long_dest = all_data[atoi(entry.airport_ID_dest)].longitude;

        if(atol(entry.dep_time) < atol(entry.arr_time))
        {
            //strcpy(str,entry.date);
            //strcat(str,entry.arr_time);
            //record.arr_time    = atol(str);
            myclock2.tm_hour  = atoi(entry.arr_time)/100;
            myclock2.tm_min   = atoi(entry.arr_time)%100;
            record.arr_time  = mktime(&myclock2);
            //printf("Hour %02d Min %02d         ",myclock2.tm_hour,myclock2.tm_min);
        }
        else
        {
            //long temp = atol(entry.date);
            //temp = temp + 1;
            //sprintf(str,"%ld",temp);
            //strcat(str,entry.arr_time);
            //record.arr_time    = atol(str);
            myclock2.tm_mday = myclock2.tm_mday + 1;
            myclock2.tm_hour  = atoi(entry.arr_time)/100;
            myclock2.tm_min   = atoi(entry.arr_time)%100;
            record.arr_time  = mktime(&myclock2);
            //printf("Hour %02d Min %02d (ADDED) ",myclock2.tm_hour,myclock2.tm_min);
        }

        record.distance = atof(entry.distance);

        strcpy(str,""); // Empty line
        zmq_send (push_socket, str, strlen(str), ZMQ_SNDMORE);

        strcpy(str,""); // Empty line for CLIENT ID
        zmq_send (push_socket, str, strlen(str), ZMQ_SNDMORE);

        strcpy(str,""); // Another Empty line
        zmq_send (push_socket, str, strlen(str), ZMQ_SNDMORE);

        strcpy(str,"INSERT"); // INSERT command
        zmq_send (push_socket, str, strlen(str), ZMQ_SNDMORE);


        // DATA
        zmq_send (push_socket, &record.lat_origin, sizeof(long), ZMQ_SNDMORE);
        zmq_send (push_socket, &record.long_origin, sizeof(long), ZMQ_SNDMORE);
        zmq_send (push_socket, &record.dep_time, sizeof(long), ZMQ_SNDMORE);

        zmq_send (push_socket, &record.lat_dest, sizeof(long), ZMQ_SNDMORE);
        zmq_send (push_socket, &record.long_dest, sizeof(long), ZMQ_SNDMORE);
        zmq_send (push_socket, &record.arr_time, sizeof(long), ZMQ_SNDMORE);

        zmq_send (push_socket, &record.airport_ID_origin, sizeof(long), ZMQ_SNDMORE);
        zmq_send (push_socket, &record.airport_ID_dest, sizeof(long), ZMQ_SNDMORE);

        // Weight
        zmq_send (push_socket, &record.distance, sizeof(double), 0);

/*
        token = asctime(localtime(&record.dep_time));
        char first[255];
        strcpy(first,token);
        first[strlen(first)-1] = '\0';
        token = asctime(localtime(&record.arr_time));
        char second[255];
        strcpy(second,token);
        second[strlen(second)-1] = '\0';

        printf(" Sent one record: %ld, %ld, %ld, ..... %s, %s\n",record.dep_time,record.arr_time, record.arr_time - record.dep_time, first, second );
*/
        printf("Sent one record: %ld, %ld, %ld, %ld, %ld, %ld, %lf (%ld,%ld)\n",record.lat_origin,record.long_origin,record.dep_time,record.lat_dest,record.long_dest,record.arr_time,record.distance,record.airport_ID_origin,record.airport_ID_dest);

        // Sleep for some time
        sleep(delay);
    }

    fclose(fp);
    zmq_close (push_socket);
    zmq_ctx_destroy (context);

    return 0;
}
