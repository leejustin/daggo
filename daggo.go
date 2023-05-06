package daggo

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"reflect"
)

// DagNode represents a node in the DAG.
type DagNode interface {
	GetID() int
	GetParentID() sql.NullInt64
	GetChildID() sql.NullInt64
	GetRootID() int
}

// Daggo is a wrapper around sqlx.DB object
type Daggo struct {
	db       *sqlx.DB
	nodeType reflect.Type
}

// NewDaggo creates a new Daggo object given a DSN
func NewDaggo(dsn string, node DagNode) (*Daggo, error) {
	if dsn == "" {
		return nil, errors.New("DSN cannot be empty")
	}

	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		return nil, err
	}

	return &Daggo{db: db, nodeType: reflect.TypeOf(node)}, nil
}

// Close closes the underlying database connection
func (d *Daggo) Close() error {
	return d.db.Close()
}

// GetNextChildrenNodes returns immediate children node(s) of the given node (eg. 1 level down)
func (d *Daggo) GetNextChildrenNodes(nodeID int) ([]DagNode, error) {
	var nodes []DagNode

	// Query the database for all nodes that have the given nodeID as their parent_id
	query := "SELECT * FROM dag WHERE parent_id = $1 ORDER BY id ASC"
	err := sqlx.Select(d.db, &nodes, query, nodeID)
	if err != nil {
		return nil, err
	}

	return nodes, nil
}

func (d *Daggo) GetParentNode(nodeID int) (*DagNode, error) {
	var node DagNode

	// Query the database for the parent of the node with the given nodeID
	query := "SELECT * FROM dag WHERE child_id = $1"
	err := d.db.Get(&node, query, nodeID)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get parent node: no rows found")
	} else if err != nil {
		return nil, fmt.Errorf("failed to get parent node: %v", err)
	}

	return &node, nil
}

// GetRootNode returns the root node of the given node
func (d *Daggo) GetRootNode(nodeID int) (*DagNode, error) {
	var node DagNode

	// Query the database for the root of the node with the given nodeID
	query := "SELECT * FROM dag WHERE root_id = $1"
	err := d.db.Get(&node, query, nodeID)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("no root node found for node %d", nodeID)
	} else if err != nil {
		return nil, err
	}
	return &node, nil
}
