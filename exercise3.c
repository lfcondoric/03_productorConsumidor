#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <semaphore.h>
#include <unistd.h>

#define BUFF_SIZE   5		/* total number of slots */
#define NP          3		/* total number of producers */
#define NC          3		/* total number of consumers */
#define NITERS      4		/* number of items produced/consumed */

typedef struct {
    int buf[BUFF_SIZE];   /* shared var */
    int in;         	  /* buf[in%BUFF_SIZE] is the first empty slot */
    int out;        	  /* buf[out%BUFF_SIZE] is the first full slot */
    sem_t full;     	  /* keep track of the number of full spots */
    sem_t empty;    	  /* keep track of the number of empty spots */
    sem_t mutex;    	  /* enforce mutual exclusion to shared data */
} sbuf_t;

sbuf_t shared;

void *Producer(void *arg)
{
    int i, item;

    long index = (long)arg;

    for (i=0; i < NITERS; i++) {

        /* Produce item */
        item = i;

        /* Prepare to write item to buf */

        /* If there are no empty slots, wait */
        sem_wait(&shared.empty);
        /* If another thread uses the buffer, wait */
        sem_wait(&shared.mutex);
        shared.buf[shared.in] = item;
        shared.in = (shared.in+1)%BUFF_SIZE;
        printf("[P%ld] Producing %d ...\n", index, item);
	fflush(stdout);
        /* Release the buffer */
        sem_post(&shared.mutex);
        /* Increment the number of full slots */
        sem_post(&shared.full);

        /* Interleave  producer and consumer execution */
        if (i % 2 == 1) sleep(1);
    }
    return NULL;
}

void *Consumer(void *arg)
{
    	int i,item;
	long index = (long)arg;

	for (i=0;i<NITERS;i++) {

        /* semWait(n): Espera a que haya elementos llenos */
        sem_wait(&shared.full);

        /* semWait(s): Exclusión mutua para la sección crítica */
        sem_wait(&shared.mutex);

        /* Extraer el ítem del búfer */
        item = shared.buf[shared.out];
        shared.out = (shared.out + 1) % BUFF_SIZE;
        printf("[C%ld] Consuming %d ...\n", index, item); 
        fflush(stdout);

        /* semSignal(s): Libera la sección crítica */
        sem_post(&shared.mutex);

        /* semSignal(e): Incrementa el número de huecos vacíos */
        sem_post(&shared.empty);

        /* Simulación de tiempo de consumo */
        if (i%2==1) sleep(1);
    }
    return NULL;
}

int main()
{
    pthread_t idP[NP], idC[NC];
    long index;

	shared.in = 0;
	shared.out = 0;

    sem_init(&shared.full, 0, 0);
    sem_init(&shared.empty, 0, BUFF_SIZE);

    /* Insert code here to initialize mutex*/
	sem_init(&shared.mutex, 0, 1);

    for (index = 0; index < NP; index++)
    {
       /* Create a new producer */
       pthread_create(&idP[index], NULL, Producer, (void*)index);
    }

	/* Crear hilos consumidores */
    for (index = 0; index < NC; index++)
    {
       pthread_create(&idC[index], NULL, Consumer, (void*)index);
    }

    /* Insert code here to create NC consumers */

    pthread_exit(NULL);
}
