package daggo

import "database/sql"

// DagNode represents a node in the DAG.
type DagNode struct {
	ID       int
	ParentID sql.NullInt64
	ChildIDs []int
	RootID   int
}

// GetID returns the ID of the node.
func (n *DagNode) GetID() int {
	return n.ID
}

// GetParentID returns the ID of the parent node.
func (n *DagNode) GetParentID() int {
	if n.ParentID.Valid {
		return int(n.ParentID.Int64)
	}
	return -1
}

// GetChildIDs returns a slice of child node IDs.
func (n *DagNode) GetChildIDs() []int {
	return n.ChildIDs
}

// GetRootID returns the ID of the root node.
func (n *DagNode) GetRootID() int {
	return n.RootID
}

// Dag represents a tree structure of DagNodes
type Dag struct {
	Root  *DagNode
	Nodes map[int][]*DagNode
}
