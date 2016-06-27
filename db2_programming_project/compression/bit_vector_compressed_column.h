#pragma once

#include <core/compressed_column.hpp>

namespace CoGaDB{


/*!
 *  \brief     This class represents a bit vector compressed column with type T, is the base class for all compressed typed column classes.
 */
template<class T>
class BitVectorCompressedColumn : public CompressedColumn<T>{
public:
    /***************** constructors and destructor *****************/
    BitVectorCompressedColumn(const std::string& name, AttributeType db_type);
    virtual ~BitVectorCompressedColumn();

    virtual bool insert(const boost::any& new_Value);
    virtual bool insert(const T& new_value);
    template <typename InputIterator>
    bool insert(InputIterator first, InputIterator last);

    virtual bool update(TID tid, const boost::any& new_value);
    virtual bool update(PositionListPtr tid, const boost::any& new_value);

    virtual bool remove(TID tid);
    //assumes tid list is sorted ascending
    virtual bool remove(PositionListPtr tid);
    virtual bool clearContent();

    virtual const boost::any get(TID tid);
    //virtual const boost::any* const getRawData()=0;
    virtual void print() const throw();
    virtual size_t size() const throw();
    virtual unsigned int getSizeinBytes() const throw();

    virtual const ColumnPtr copy() const;

    virtual bool store(const std::string& path);
    virtual bool load(const std::string& path);



    virtual T& operator[](const int index);

    /*! values*/
    std::pair<std::vector<T>, std::vector<std::string> > bitVectorPair;
    std::string _name;

};


/***************** Start of Implementation Section ******************/


    template<class T>
    BitVectorCompressedColumn<T>::BitVectorCompressedColumn(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type), bitVectorPair(), _name(name) {

    }

    template<class T>
    BitVectorCompressedColumn<T>::~BitVectorCompressedColumn(){

    }

    template<class T>
    bool BitVectorCompressedColumn<T>::insert(const boost::any&){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
        return false;
    }

    template<class T>
    bool BitVectorCompressedColumn<T>::insert(const T& new_value) {
        if(bitVectorPair.first.size() == 0) {
            bitVectorPair.first.push_back(new_value);
            bitVectorPair.second.push_back("1");
            return true;
        }

        bool wasInserted = false;
        for(unsigned int i = 0; i < bitVectorPair.first.size(); i++) {
            if(bitVectorPair.first[i] == new_value) {
                bitVectorPair.second[i].push_back('1');
                wasInserted = true;
            } else {
                bitVectorPair.second[i].push_back('0');
            }
        }

        if(wasInserted == false) {
            bitVectorPair.first.push_back(new_value);

            std::string stringValue = "";
            for(unsigned int i = 0; i < bitVectorPair.second[0].length() - 1; i++) {
                stringValue.push_back('0');
            }
            stringValue.push_back('1');
            bitVectorPair.second.push_back(stringValue);
        }

        return true;
    }

    template <typename T>
    template <typename InputIterator>
    bool BitVectorCompressedColumn<T>::insert(InputIterator, InputIterator){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
        return true;
    }

    template<class T>
    const boost::any BitVectorCompressedColumn<T>::get(TID){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
        return boost::any();
    }

    template<class T>
    void BitVectorCompressedColumn<T>::print() const throw(){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
    }

    template<class T>
    size_t BitVectorCompressedColumn<T>::size() const throw(){
        int counter = 0;
        for(unsigned int i = 0; i < bitVectorPair.first.size(); i++) {
            for(unsigned int j = 0; j < bitVectorPair.second[i].length(); j++) {
                if(bitVectorPair.second[i][j] == '1') {
                    counter++;
                }
            }
        }

        return counter;
    }

    template<class T>
    const ColumnPtr BitVectorCompressedColumn<T>::copy() const{
        return ColumnPtr(new BitVectorCompressedColumn<T>(*this));
    }

    template<class T>
    bool BitVectorCompressedColumn<T>::update(TID tid, const boost::any& new_value){
        if(bitVectorPair.second.size() == 0 || tid > bitVectorPair.second[0].length()) {
            return false;
        }

        for(unsigned int i = 0; i < bitVectorPair.second.size(); i++) {
            if(bitVectorPair.second[i][tid] == '1') {
                bitVectorPair.second[i][tid] = '0';
            }
        }

        for(unsigned int i = 0; i < bitVectorPair.first.size(); i++) {
            if(bitVectorPair.first[i] == boost::any_cast<T>(new_value)) {
                bitVectorPair.second[i][tid] = '1';
                return true;
            }
        }

        bitVectorPair.first.push_back(boost::any_cast<T>(new_value));

        std::string stringValue = "";
        for(unsigned int i = 0; i < bitVectorPair.second[0].length(); i++) {
            if(i == tid) {
                stringValue.push_back('1');
            } else {
                stringValue.push_back('0');
            }
        }
        bitVectorPair.second.push_back(stringValue);

        return true;
    }

    template<class T>
    bool BitVectorCompressedColumn<T>::update(PositionListPtr, const boost::any&){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
        return false;
    }

    template<class T>
    bool BitVectorCompressedColumn<T>::remove(TID tid){
        for(unsigned int i = 0; i < bitVectorPair.second.size(); i++) {
            bitVectorPair.second[i].erase(bitVectorPair.second[i].begin() + tid);
        }

        return true;
    }

    template<class T>
    bool BitVectorCompressedColumn<T>::remove(PositionListPtr){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
        return false;
    }

    template<class T>
    bool BitVectorCompressedColumn<T>::clearContent(){
        bitVectorPair.first.clear();
        bitVectorPair.second.clear();
        return true;
    }

    template<class T>
    bool BitVectorCompressedColumn<T>::store(const std::string& path_){
        std::string path(path_);
        path += _name;

        std::ofstream outfile (path.c_str(), std::fstream::out | std::fstream::binary);
        boost::archive::binary_oarchive oa(outfile);

        oa << bitVectorPair;
        outfile.flush();
        outfile.close();

        return true;
    }

    template<class T>
    bool BitVectorCompressedColumn<T>::load(const std::string& path_){
        std::string path(path_);
        path += _name;

        std::ifstream infile (path.c_str(), std::fstream::in | std::fstream::binary);
        boost::archive::binary_iarchive ia(infile);

        ia >> bitVectorPair;
        infile.close();

        return true;
    }

    template<class T>
    T& BitVectorCompressedColumn<T>::operator[](const int index){
        for(unsigned int i = 0; i < bitVectorPair.second.size(); i++) {
            if(bitVectorPair.second[i][index] == '1') {
                return bitVectorPair.first[i];
            }
        }

        // Will result in an exception (out of bounds). This will point to a problem in this method (if there should be one).
        return bitVectorPair.first[bitVectorPair.first.size()];
    }

    template<class T>
    unsigned int BitVectorCompressedColumn<T>::getSizeinBytes() const throw(){
        int size = bitVectorPair.first.capacity() * sizeof(T);
        size += bitVectorPair.second.capacity() * sizeof(std::string);
        return size;
    }

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB

