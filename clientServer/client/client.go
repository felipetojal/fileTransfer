package client

import(
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type resultado struct {
	seg float64
	bytes int64
}

// Funcao para baixar o arquivo
func baixar(addr string) (resultado, error){
	// Conexão
	inicio := time.Now()
	coon, err := net.Dial("tcp", addr)
	if err != nil{
		return resultado{}, err
	}
	defer coon.Close()

	var total int64
	// Leitura do arquivo
	err = binary.Read(coon, binary.BigEndian, &total)
	if err != nil{
		return resultado {}, err
	}
	lidos, err := io.CopyN(io.Discard, coon, total)
	if err != nil{
		return resultado {}, err
	}

	// Retorna o tempo da leitura do arquivo e o número de bits
	return resultado{time.Since(inicio).Seconds(), lidos}, nil
}


func main(){
	host := flag.String("host", "localhost", "host do servidor")
	porta := flag.Int("porta", 5000, "porta do servidor")
	n := flag.Int("n", 1, "numero de clientes simultaneos")
	flag.Parse()
	addr := fmt.Sprint("%s:%d", *host, *porta)

	// Pega os arquivos enviados e processa
	res := make([]resultado, *n)
	var wg sync.WaitGroup
	largada := make(chan struct {})
	for i := 0; i< *n; i++ {
		wg.Add(1)
		go func(i int){
			defer wg.Done()
			<-largada
			r, err := baixar(addr)
			if err != nil {
				log.Printf("cliente %d: %v", i, err)
			}
			res[i]=r
		}(i)
	}
	close(largada)
	wg.Wait()

	
	// Pegar o tempo minimo/medio/maximo da leitura do arquivo
	var soma, max float64
	min := 10000000.0
	for _, r := range res {
		soma += r.seg
		if r.seg > max {
			max = r.seg
		}
		if r.seg < min {
			min = r.seg
		}
	}
	fmt.Printf("Tempo minimo: %.3f", min)
	fmt.Printf("Tempo médio: %.3f", soma/float64(*n))
	fmt.Printf("Tempo maximo: %.3f", max)
}
