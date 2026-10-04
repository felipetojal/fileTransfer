package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"sync"
	"time"
)

type resultado struct {
	seg   float64
	bytes int64
	ok    bool
}

// Funcao para baixar o arquivo
func baixar(addr string) (resultado, error) {
	// Conexão
	inicio := time.Now()
	coon, err := net.Dial("tcp", addr)
	if err != nil {
		return resultado{}, err
	}
	defer coon.Close()

	var total int64
	// Leitura do arquivo
	err = binary.Read(coon, binary.BigEndian, &total)
	if err != nil {
		return resultado{}, err
	}
	lidos, err := io.CopyN(io.Discard, coon, total)
	if err != nil {
		return resultado{}, err
	}

	// Retorna o tempo da leitura do arquivo e o número de bits
	return resultado{time.Since(inicio).Seconds(), lidos, true}, nil
}

func main() {
	host := flag.String("host", "localhost", "host do servidor")
	porta := flag.Int("porta", 5000, "porta do servidor")
	n := flag.Int("n", 1, "numero de clientes simultaneos")
	flag.Parse()
	addr := fmt.Sprintf("%s:%d", *host, *porta)

	// Pega os arquivos enviados e processa
	res := make([]resultado, *n)
	var wg sync.WaitGroup
	largada := make(chan struct{})
	for i := 0; i < *n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-largada
			r, err := baixar(addr)
			if err != nil {
				log.Printf("cliente %d: %v", i, err)
			}
			res[i] = r
		}(i)
	}
	close(largada)
	wg.Wait()

	// Pegar o tempo minimo/medio/maximo da leitura do arquivo
	var soma, max float64
	min := math.Inf(1)
	validos := 0
	for _, r := range res {
		if !r.ok {
			continue
		}
		validos++
		soma += r.seg
		max = math.Max(max, r.seg)
		min = math.Min(min, r.seg)
	}
	if validos == 0 {
		log.Fatal("nenhum download concluiu")
	}
	fmt.Printf("válidos=%d/%d min=%.3f med=%.3f max=%.3f\n",
		validos, *n, min, soma/float64(validos), max)
}
