package cadreaderv2

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSomething(t *testing.T) {
	assert.True(t, true, "True is true!")
}

func TestProcedure(t *testing.T) {
	_, err := procedure("internal/assets/CAD01.dbf", "internal/assets/CAD01_copia.dbf")
	assert.NoError(t, err)
}

func TestProcedureProcurarInsert(t *testing.T) {
	result := procedureProcurarInsert("internal/assets/CAD01.dbf", "internal/assets/CAD01_copia.dbf")
	assert.NotEmpty(t, result)
}

// "internal/assets/CAD01.DBF"

func TestProcurarCad01PorInscr(t *testing.T) {
	_, err := ProcurarCad01PorInscr("internal/assets/CAD01.DBF", "0")
	assert.Error(t, err)
	result, err := ProcurarCad01PorInscr("internal/assets/CAD01.DBF", "200257980")
	assert.NoError(t, err)
	fmt.Printf("result: %v\n", result)
}

func TestGetCad(t *testing.T) {
	cad, err := GetCad01Modelo("internal/assets/CAD01.DBF", "200257980")
	assert.NoError(t, err)
	// fmt.Printf("cad: %v\n", cad)
	fmt.Printf("cad.Depro: %v\n", cad.Depro)
}
