db = db.getSiblingDB('hypha');
db.auth('hypha', 'thisisnotarealpassword');
db.createCollection('test');

db.test.insertOne({
  personalInfo: {
    name: {
      first: 'John',
      last: 'Doe',
      middleInitial: 'A'
    },
    age: 30,
    contact: {
      email: 'john.doe@example.com',
      phone: {
        home: '+1-555-123-4567',
        mobile: '+1-555-987-6543'
      },
      address: {
        street: {
          number: '123',
          name: 'Main St'
        },
        city: 'Anytown',
        state: 'CA',
        zipCode: '12345',
        country: 'USA'
      }
    }
  },
  employment: {
    currentJob: {
      title: 'Software Engineer',
      company: 'Tech Innovations Inc.',
      startDate: new Date('2020-01-15'),
      department: {
        name: 'Web Development',
        floor: 3,
        manager: {
          name: 'Jane Smith',
          employeeId: 'JS001'
        }
      }
    },
    previousJobs: [
      {
        title: 'Junior Developer',
        company: 'StartUp Co.',
        period: {
          start: new Date('2018-06-01'),
          end: new Date('2019-12-31')
        }
      }
    ]
  },
  skills: {
    programming: ['JavaScript', 'Python', 'Java'],
    languages: [
      {
        name: 'English',
        proficiency: 'Native'
      },
      {
        name: 'Spanish',
        proficiency: 'Intermediate'
      }
    ],
    softSkills: ['Communication', 'Teamwork', 'Problem-solving']
  },
  interests: {
    hobbies: ['Photography', 'Hiking', 'Cooking'],
    volunteering: {
      organization: 'Local Food Bank',
      hours: 5,
      frequency: 'Weekly'
    }
  }
});
