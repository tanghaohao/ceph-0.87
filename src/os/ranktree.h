#ifndef _RED_BLACK_TREE_HPP_
#define _RED_BLACK_TREE_HPP_ 

#include <iomanip>
#include <iostream>
#include <deque>
using namespace std;

enum RBTColor{RED, BLACK};

template <class T>
class Node{
    public:
	T key;
        RBTColor color;
        Node *parent;
        Node *left;
        Node *right;
	int rank;
	
	Node(T value, RBTColor c, Node *p, Node *l, Node *r):
	    key(value),color(c),parent(),left(l),right(r),rank(0) {}
};

template <class T>
class RankTree {
    private:
	Node<T> *mRoot;
	int mSize;

    public:
	RankTree();
	~RankTree();

	typedef Node<T> node_type;
	typedef T key_type;

	T rank2key(int rank);
	int size() { return mSize; }

	int insert(T key);
	int remove(T key);

	Node<T>* next(Node<T> *current = NULL);
	Node<T>* search(T key, int *rank = NULL);
	void destroy();
    private:
	void leftRotate(Node<T>* &root, Node<T>* x);
	void rightRotate(Node<T>* &root, Node<T>* y);
	int insert(Node<T>* &root, Node<T>* node);
	void insertFixUp(Node<T>* &root, Node<T>* node);
	void remove(Node<T>* &root, Node<T> *node);
	void removeFixUp(Node<T>* &root, Node<T> *node, Node<T> *parent);
	void destroy(Node<T>* &tree);

#define rb_parent(r)   ((r)->parent)
#define rb_color(r) ((r)->color)
#define rb_is_red(r)   ((r)->color==RED)
#define rb_is_black(r)  ((r)->color==BLACK)
#define rb_set_black(r)  do { (r)->color = BLACK; } while (0)
#define rb_set_red(r)  do { (r)->color = RED; } while (0)
#define rb_set_parent(r,p)  do { (r)->parent = (p); } while (0)
#define rb_set_color(r,c)  do { (r)->color = (c); } while (0)
};

template <class T>
RankTree<T>::RankTree():mRoot(NULL)
{
    mRoot = NULL;
    mSize = 0;
}

template <class T>
RankTree<T>::~RankTree() 
{
    destroy();
}

template <class T>
T RankTree<T>::rank2key(int rank)
{
    if (rank >= 0) {
	Node<T>* x = mRoot;
	while (x) {
	    if (x->rank < rank) {
		rank -= x->rank + 1;
		x = x->right;
	    }
	    else if (x->rank > rank) {
		x = x->left;
	    }
	    else {
		return x->key;
	    }
	}
    }
    return T();
}

/* 
 * 对红黑树的节点(x)进行左旋转
 *
 * 左旋示意图(对节点x进行左旋)：
 *      px                              px
 *     /                               /
 *    x                               y                
 *   /  \      --(左旋)-->           / \                #
 *  lx   y                          x  ry     
 *     /   \                       /  \
 *    ly   ry                     lx  ly  
 *
 *
 */
template <class T>
void RankTree<T>::leftRotate(Node<T>* &root, Node<T>* x)
{
    Node<T> *y = x->right;
    x->right = y->left;
    if (y->left != NULL)
        y->left->parent = x;

    y->parent = x->parent;

    if (x->parent == NULL)
    {
        root = y;
    }
    else
    {
        if (x->parent->left == x)
            x->parent->left = y;
        else
            x->parent->right = y;
    }
    
    y->left = x;
    x->parent = y;
    y->rank += x->rank + 1;
}

/* 
 * 对红黑树的节点(y)进行右旋转
 *
 * 右旋示意图(对节点y进行左旋)：
 *            py                               py
 *           /                                /
 *          y                                x                  
 *         /  \      --(右旋)-->            /  \                     #
 *        x   ry                           lx   y  
 *       / \                                   / \                   #
 *      lx  rx                                rx  ry
 * 
 */
template <class T>
void RankTree<T>::rightRotate(Node<T>* &root, Node<T>* y)
{
    Node<T> *x = y->left;

    y->left = x->right;
    if (x->right != NULL)
        x->right->parent = y;

    x->parent = y->parent;

    if (y->parent == NULL) 
    {
        root = x;
    }
    else
    {
        if (y == y->parent->right)
            y->parent->right = x;
        else
            y->parent->left = x;
    }

    x->right = y;
    y->parent = x;
    y->rank -= x->rank + 1;
}

template <class T>
void RankTree<T>::insertFixUp(Node<T>* &root, Node<T>* node)
{
    Node<T> *parent, *gparent;

    while ((parent = rb_parent(node)) && rb_is_red(parent))
    {
        gparent = rb_parent(parent);

        //若“父节点”是“祖父节点的左孩子”
        if (parent == gparent->left)
        {
            // Case 1条件：叔叔节点是红色
            {
                Node<T> *uncle = gparent->right;
                if (uncle && rb_is_red(uncle))
                {
                    rb_set_black(uncle);
                    rb_set_black(parent);
                    rb_set_red(gparent);
                    node = gparent;
                    continue;
                }
            }

            // Case 2条件：叔叔是黑色，且当前节点是右孩子
            if (parent->right == node)
            {
                Node<T> *tmp;
                leftRotate(root, parent);
                tmp = parent;
                parent = node;
                node = tmp;
            }

            // Case 3条件：叔叔是黑色，且当前节点是左孩子。
            rb_set_black(parent);
            rb_set_red(gparent);
            rightRotate(root, gparent);
        } 
        else//若“z的父节点”是“z的祖父节点的右孩子”
        {
            // Case 1条件：叔叔节点是红色
            {
                Node<T> *uncle = gparent->left;
                if (uncle && rb_is_red(uncle))
                {
                    rb_set_black(uncle);
                    rb_set_black(parent);
                    rb_set_red(gparent);
                    node = gparent;
                    continue;
                }
            }

            // Case 2条件：叔叔是黑色，且当前节点是左孩子
            if (parent->left == node)
            {
                Node<T> *tmp;
                rightRotate(root, parent);
                tmp = parent;
                parent = node;
                node = tmp;
            }

            // Case 3条件：叔叔是黑色，且当前节点是右孩子。
            rb_set_black(parent);
            rb_set_red(gparent);
            leftRotate(root, gparent);
        }
    }

    // 将根节点设为黑色
    rb_set_black(root);
}

/* 
 * 将结点插入到红黑树中
 *
 * 参数说明：
 *     root 红黑树的根结点
 *     node 插入的结点
 * 返回值：
 *     node 在红黑树中的排名
 */
template <class T>
int RankTree<T>::insert(Node<T>* &root, Node<T>* node)
{
    Node<T> *y = NULL;
    Node<T> *x = root;
    deque< Node<T>* > path;
    int rank = 0;

    while (x != NULL)
    {
        y = x;
	if (node->key == x->key)
	    return -1;
	else if (node->key < x->key){
	    path.push_back(y);
            x = x->left;
	} else {
	    rank += x->rank + 1;
            x = x->right;
	}
    }

    node->parent = y;
    if (y!=NULL)
    {
        if (node->key < y->key)
            y->left = node;
        else
            y->right = node;
    }
    else
        root = node;

    while (!path.empty()) {
	Node<T> *p = path.front();
	path.pop_front();
	p->rank ++;
    }
    node->color = RED;

    insertFixUp(root, node);
    return rank;
}

template <class T>
int RankTree<T>::insert(T key)
{
    Node<T> *z=NULL;

    if ((z=new Node<T>(key,BLACK,NULL,NULL,NULL)) == NULL)
        return -1;

    int rank = insert(mRoot, z);
    if (rank == -1)
	delete z;
    else
	mSize += 1;
    return rank;
}

/*
 * 红黑树删除修正函数
 *
 * 在从红黑树中删除插入节点之后(红黑树失去平衡)，再调用该函数；
 * 目的是将它重新塑造成一颗红黑树。
 *
 * 参数说明：
 *     root 红黑树的根
 *     node 待修正的节点
 */
template <class T>
void RankTree<T>::removeFixUp(Node<T>* &root, Node<T> *node, Node<T> *parent)
{
    Node<T> *other;

    while ((!node || rb_is_black(node)) && node != root)
    {
        if (parent->left == node)
        {
            other = parent->right;
            if (rb_is_red(other))
            {
                // Case 1: x的兄弟w是红色的  
                rb_set_black(other);
                rb_set_red(parent);
                leftRotate(root, parent);
                other = parent->right;
            }
            if ((!other->left || rb_is_black(other->left)) &&
                (!other->right || rb_is_black(other->right)))
            {
                // Case 2: x的兄弟w是黑色，且w的俩个孩子也都是黑色的  
                rb_set_red(other);
                node = parent;
                parent = rb_parent(node);
            }
            else
            {
                if (!other->right || rb_is_black(other->right))
                {
                    // Case 3: x的兄弟w是黑色的，并且w的左孩子是红色，右孩子为黑色。  
                    rb_set_black(other->left);
                    rb_set_red(other);
                    rightRotate(root, other);
                    other = parent->right;
                }
                // Case 4: x的兄弟w是黑色的；并且w的右孩子是红色的，左孩子任意颜色。
                rb_set_color(other, rb_color(parent));
                rb_set_black(parent);
                rb_set_black(other->right);
                leftRotate(root, parent);
                node = root;
                break;
            }
        }
        else
        {
            other = parent->left;
            if (rb_is_red(other))
            {
                // Case 1: x的兄弟w是红色的  
                rb_set_black(other);
                rb_set_red(parent);
                rightRotate(root, parent);
                other = parent->left;
            }
            if ((!other->left || rb_is_black(other->left)) &&
                (!other->right || rb_is_black(other->right)))
            {
                // Case 2: x的兄弟w是黑色，且w的俩个孩子也都是黑色的  
                rb_set_red(other);
                node = parent;
                parent = rb_parent(node);
            }
            else
            {
                if (!other->left || rb_is_black(other->left))
                {
                    // Case 3: x的兄弟w是黑色的，并且w的左孩子是红色，右孩子为黑色。  
                    rb_set_black(other->right);
                    rb_set_red(other);
                    leftRotate(root, other);
                    other = parent->left;
                }
                // Case 4: x的兄弟w是黑色的；并且w的右孩子是红色的，左孩子任意颜色。
                rb_set_color(other, rb_color(parent));
                rb_set_black(parent);
                rb_set_black(other->left);
                rightRotate(root, parent);
                node = root;
                break;
            }
        }
    }
    if (node)
        rb_set_black(node);
}

/* 
 * 删除结点(node)，并返回被删除的结点
 *
 * 参数说明：
 *     root 红黑树的根结点
 *     node 删除的结点
 */
template <class T>
void RankTree<T>::remove(Node<T>* &root, Node<T> *node)
{
    Node<T> *child, *parent;
    RBTColor color;

    if ( (node->left!=NULL) && (node->right!=NULL) ) 
    {
        Node<T> *replace = node->right;
        while (replace->left != NULL)
            replace = replace->left;
	
	{
	    parent = replace;
	    while (rb_parent(parent)) {
		if (rb_parent(parent)->left == parent)
		    rb_parent(parent)->rank --;
		parent = rb_parent(parent);
	    }
	}

        if (rb_parent(node))
        {
            if (rb_parent(node)->left == node)
                rb_parent(node)->left = replace;
            else
                rb_parent(node)->right = replace;
        } 
        else 
            root = replace;

        child = replace->right;
        parent = rb_parent(replace);
        color = rb_color(replace);

        if (parent == node)
        {
            parent = replace;
        } 
        else
        {
            if (child)
                rb_set_parent(child, parent);
            parent->left = child;

            replace->right = node->right;
            rb_set_parent(node->right, replace);
        }

        replace->parent = node->parent;
        replace->color = node->color;
        replace->left = node->left;
	replace->rank = node->rank;
        node->left->parent = replace;

        if (color == BLACK)
            removeFixUp(root, child, parent);

        delete node;
        return ;
    }

    if (node->left !=NULL)
        child = node->left;
    else 
        child = node->right;

    parent = node;
    while (rb_parent(parent)) {
	if (rb_parent(parent)->left == parent)
	    rb_parent(parent)->rank --;
	parent = rb_parent(parent);
    }

    parent = node->parent;
    color = node->color;

    if (child)
        child->parent = parent;

    if (parent)
    {
        if (parent->left == node)
            parent->left = child;
        else
            parent->right = child;
    }
    else
        root = child;

    if (color == BLACK)
        removeFixUp(root, child, parent);
    delete node;
}

template <class T>
Node<T>* RankTree<T>::next(Node<T>* current)
{
    Node<T>* ret;
    if (!current) {
	ret = mRoot;
	while (ret->left)
	    ret = ret->left;
    }
    else if (current->right) {
	ret = current->right;
	while (ret->left)
	    ret = ret->left;
    }
    else {
	ret = rb_parent(current);
	while (ret && current == ret->right) {
	    current = ret;
	    ret = rb_parent(current);
	}
    }
    return ret;
}
/* 
 * 删除红黑树中键值为key的节点
 *
 * 参数说明：
 *     tree 红黑树的根结点
 */
template <class T>
Node<T>* RankTree<T>::search(T key, int *rank)
{
    Node<T> *x = mRoot;
    int nrank = 0;

    while (x) {
	if (x->key == key) {
	    nrank += x->rank;
	    break;
	}
	else if (x->key < key) {
	    nrank += x->rank + 1;
	    x = x->right;
	}
	else
	    x = x->left;
    }
    if (rank)
	*rank = nrank;
    return x;
}

template <class T>
int RankTree<T>::remove(T key)
{
    Node<T> *node;
    int rank;

    if ((node = search(key, &rank)) != NULL) {
	mSize -= 1;
	remove(mRoot, node);
	return rank;
    }
    return -1;
}

/*
 * 销毁红黑树
 */
template <class T>
void RankTree<T>::destroy(Node<T>* &tree)
{
    if (tree==NULL)
        return ;

    if (tree->left != NULL)
        return destroy(tree->left);
    if (tree->right != NULL)
        return destroy(tree->right);

    delete tree;
    tree=NULL;
    mSize = 0;
}

template <class T>
void RankTree<T>::destroy()
{
    destroy(mRoot);
}
#endif
