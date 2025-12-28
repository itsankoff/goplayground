package main

import (
	"crypto/md5"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"errors"
)

func gen(nums ...int) <-chan int {
	out := make(chan int)
	go func() {
		for _, n := range nums {
			out <- n
		}
		close(out)
	}()

	return out
}

func sq(done <-chan struct{}, in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for n := range in {
			select {
			case out <- n * n:
			case <-done:
				return
			}
		}
	}()

	return out
}

func merge(done <-chan struct{}, ins ...<-chan int) <-chan int {
	var wg sync.WaitGroup
	out := make(chan int)

	output := func(c <-chan int) {
		defer wg.Done()
		for n := range c {
			select {
			case out <- n:
			case <-done:
				return
			}
			out <- n
		}
	}

	wg.Add(len(ins))
	for _, in := range ins {
		go output(in)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func sumFile(path string) ([md5.Size]byte, error) {
	zero := [16]byte{0}

	data, err := os.ReadFile(path)
	if err != nil {
		return zero, err
	}

	return md5.Sum(data), nil
}

func MD5All(root string) (map[string][md5.Size]byte, error) {
	sums := map[string][md5.Size]byte{}

	err := filepath.Walk(root, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}

		sum, err := sumFile(path)
		if err != nil {
			return err
		}

		sums[path] = sum
		return nil
	})

	if err != nil {
		return nil, err
	}

	return sums, nil
}

type result struct {
	sum  [md5.Size]byte
	path string
}

func MD5Parallel(root string) (map[string][md5.Size]byte, error) {
	sumsChan := make(chan result)
	errsChan := make(chan error)
	doneChan := make(chan struct{})
	var wg sync.WaitGroup

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			sum, err := sumFile(path)
			sumsChan <- result{
				sum:  sum,
				path: path,
			}
			errsChan <- err
		}()

		return nil
	})

	sums := map[string][md5.Size]byte{}
	var errs error
	go func() {
		for {
			select {
			case sum := <-sumsChan:
				sums[sum.path] = sum.sum
			case err := <-errsChan:
				errs = errors.Join(errs, err)
			case <-doneChan:
				return
			}
		}
	}()

	wg.Wait()
	close(doneChan)
	return sums, err
}

func MD5Bounded(root string) (map[string][md5.Size]byte, error) {
	// gen files return channels fan out
	// run multiple workers on channels process
	// aggregate fan in

	paths := make(chan string)
	errs := make(chan error)
	go func() {
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				errs <- err
				return err
			}

			if info.Mode().IsRegular() {
				paths <- path
			}

			return nil
		})

		if err != nil {
			errs <- err
		}
	}()

	numDigesters := 10
	sums := make(chan result)
	var wg sync.WaitGroup
	for i := 0; i < numDigesters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for path := range paths {
				sum, err := sumFile(path)
				sums <- result{
					sum:  sum,
					path: path,
				}
				if err != nil {
					errs <- err
				}
			}
		}()
	}

	output := map[string][md5.Size]byte{}
	var errOut error
	for sum := range sums {
		output[sum.path] = sum.sum
	}

	for err := range errs {
		errOut = errors.Join(errOut, err)
	}

	close(paths)
	wg.Wait()

	return output, errOut
}

func main() {
	// in := gen(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	// done := make(chan struct{})
	// defer close(done)

	// c1 := sq(done, in)
	// c2 := sq(done, in)

	// for n := range merge(done, c1, c2) {
	// 	fmt.Println("number: ", n)
	// }

	m, err := MD5Bounded(os.Args[1])
	if err != nil {
		fmt.Println(err)
	}

	var paths []string
	for path, _ := range m {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	for _, path := range paths {
		fmt.Printf("path: %s, md5: %x\n", path, m[path])
	}
}
