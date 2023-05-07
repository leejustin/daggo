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

// Dag represents a tree structure of DagNodes
type Dag struct {
	Root *DagNode
	Nodes map[int]*DagNode
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

// GetDescendants returns all descendants of the given nodeID
func (d *Daggo) GetDescendants(nodeID int) ([]DagNode, error) {
	// Initialize a slice to hold the descendants
	descendants := make([]DagNode, 0)

	// Build a recursive query to fetch all descendants of the given node
	query := `
		WITH RECURSIVE cte AS (
			SELECT *
			FROM dag
			WHERE parent_id = $1
			UNION ALL
			SELECT dag.*
			FROM dag
			JOIN cte ON dag.parent_id = cte.child_id
		)
		SELECT *
		FROM cte
		ORDER BY id ASC
	`

	// Execute the query and retrieve the descendants
	err := d.db.Select(&descendants, query, nodeID)
	if err != nil {
		return nil, err
	}

	return descendants, nil
}

// GetParents returns all ancestors of the given node in a slice
func (d *Daggo) GetAncestors(nodeID int) ([]DagNode, error) {
	// Initialize a slice to hold the ancestors
	ancestors := make([]DagNode, 0)

	// Build a recursive query to fetch all ancestors of the given node
	query := `
		WITH RECURSIVE cte AS (
			SELECT *
			FROM dag
			WHERE child_id = $1
			UNION ALL
			SELECT dag.*
			FROM dag
			JOIN cte ON dag.child_id = cte.parent_id
		)
		SELECT *
		FROM cte
		ORDER BY id ASC
	`

	// Execute the query and retrieve the ancestors
	err := d.db.Select(&parents, query, nodeID)
	if err != nil {
		return nil, err
	}

	return ancestors, nil
}

// NewDag creates a new Dag object from a slice of DagNodes
func NewDag(nodes []DagNode) *Dag {
	dag := &Dag{Nodes: make(map[int]*DagNode)}

	// Create a map of nodes
	for i := range nodes {
		dag.Nodes[nodes[i].GetID()] = &nodes[i]
	}

	// Find the root node
	var root *DagNode
	for i := range nodes {
		if !nodes[i].GetParentID().Valid {
			if root != nil {
				// More than one root node exists
				return nil
			}
			root = &nodes[i]
		}
	}
	if root == nil {
		// No root node found
		return nil
	}
	dag.Root = root

	// Set the children of each node
	for i := range nodes {
		if nodes[i].GetParentID().Valid {
			parentID := nodes[i].GetParentID().Int64
			parent, ok := dag.Nodes[int(parentID)]
			if !ok {
				// Parent node not found
				return nil
			}
			parent.Children = append(parent.Children, &nodes[i])
		}
	}

	return dag
}
// GetDescendantsTree retrieves the ancestors of a given node as a tree
func (d *Daggo) GetDescendantsTree(nodeID int) (*Dag, error) {
	nodes, err := d.GetDescendants(nodeID)
	if err != nil {
		return nil, err
	}

	dag := NewDag(nodes)
	if dag == nil {
		return nil, errors.New("failed to create Dag tree")
	}

	return dag, nil
}
