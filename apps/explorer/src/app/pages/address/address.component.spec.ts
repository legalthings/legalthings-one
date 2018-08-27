import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import {
  PageContentModule,
  AmountPipeModule,
  TransactionsSectionModule,
  AMOUNT_DIVIDER
} from '@explorer/shared';
import { MatCardModule, MatProgressSpinnerModule } from '@angular/material';
import { LtoPublicNodeService } from '@legalthings-one/platform';
import { AddressComponent } from './address.component';

describe('AddressComponent', () => {
  let component: AddressComponent;
  let fixture: ComponentFixture<AddressComponent>;
  let publicNodeMock: Partial<LtoPublicNodeService>;

  beforeEach(async(() => {
    publicNodeMock = {};

    TestBed.configureTestingModule({
      imports: [
        RouterTestingModule,
        PageContentModule,
        MatCardModule,
        MatProgressSpinnerModule,
        AmountPipeModule,
        TransactionsSectionModule
      ],
      declarations: [AddressComponent],
      providers: [
        {
          provide: LtoPublicNodeService,
          useValue: publicNodeMock
        },
        {
          provide: AMOUNT_DIVIDER,
          useValue: 10000000
        }
      ]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AddressComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
